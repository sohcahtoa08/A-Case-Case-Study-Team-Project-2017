import scrapy
import psycopg2
import string
import datetime

# EXAMPLE: scrapy crawl cases -a dbhost=localhost -a db=test -a dbuser=user -a dbpassword=password -a start_date=YYYY/MM/DD -a end_date=YYYY/MM/DD

BASE_URL = 'http://casesearch.courts.state.md.us'
DISCLAIMER_URL = '/casesearch/processDisclaimer.jis'
SEARCH_URL = '/casesearch/inquirySearch.jis'

COMPANY_TYPES = ['Y', 'N']
CASE_TYPES = ['CIVIL', 'CRIMINAL', 'TRAFFIC', 'CP']
COURT_SYSTEMS = ['C', 'D']
LETTER_MAX = 26

# Compute list of dates between two
def daterange(start_date, end_date):
	if start_date <= end_date:
		for n in range((end_date - start_date).days + 1):
			yield start_date + datetime.timedelta(n)
	else:
		for n in range((start_date - end_date).days + 1):
			yield start_date - datetime.timedelta(n)

# Get the Case ID from an inquiry-details page URL
def extractCaseId(url):
	return url.split('&')[0].split('=')[1]

# Parse data from arguments
def parseDate(paramstr):
	values = paramstr.split('/')
	return datetime.date(year = int(values[0]), month = int(values[1]), day = int(values[2]))

# Create date string from date object
def combineDate(date):
	return str(date.month) + '/' + str(date.day) + '/' + str(date.year)

# Get case details HTML files
class CasesSpider(scrapy.Spider):
	name = 'cases'
	cookie = None
	conn = None
	cur = None

	# Connect to PostgreSQL and start crawler on disclaimer page
	def start_requests(self):
		self.connectToDatabase(self)

		return [ scrapy.Request(
			BASE_URL + DISCLAIMER_URL,
			callback = self.acceptDisclaimer
		)]

	# Connect to PostgreSQL
	def connectToDatabase(self, *args):
		try:
			self.conn = psycopg2.connect(host=self.dbhost, database=self.db, user=self.dbuser, password=self.dbpassword)
			self.logger.info('Connected to PostgreSQL')
		except:
			self.logger.critical('Unable to connect to PostgreSQL')
		self.cur = self.conn.cursor()

	# Spoof form submission
	def acceptDisclaimer(self, response):
		self.cookie = response.headers['Set-Cookie']

		yield scrapy.FormRequest(
			BASE_URL + DISCLAIMER_URL,
			formdata = {
				'action': 'Continue',
				'disclaimer': 'Y'
			},
			callback = self.doSearches
		 )

	# Iterate thru field ranges and get results
	def doSearches(self, response):
		for date in daterange(parseDate(self.start_date), parseDate(self.end_date)):
			dateStr = combineDate(date)
			self.logger.debug('Now searching for exact date: %s', dateStr)
			for company in COMPANY_TYPES:
				self.logger.debug('Now searching for type: %s', 'Company' if company == 'Y' else 'Person')
				for letter in range(LETTER_MAX):
					letterStr = string.ascii_lowercase[letter]
					self.logger.debug('Now searching for last name: %s', letterStr)
					for case in CASE_TYPES:
						self.logger.debug('Now searching in category: %s', case)
						for court in COURT_SYSTEMS:
							self.logger.debug('Now searching in system: %s', 'Circuit' if court == 'C' else 'District')
							yield scrapy.FormRequest(
								BASE_URL + SEARCH_URL,
								headers = {
									'Cookie': self.cookie
								},
								formdata = {
									'action': 'Search',
									'company': company,
									'countyName': '',
									'courtSystem': court,
									'filingDate': dateStr,
									'filingEnd': '',
									'filingStart': '',
									'firstName': '',
									'lastName': letterStr,
									'middleName':'',
									'partyType': '',
									'site': case,
								},
								callback = self.parseResults
							)

	# Extract case detail links from results pages
	def parseResults(self, response, *args):
		# Redo request if response was not OK
		if response.status != 200:
			yield response.request
			return
		# Look for <a> in results table
		caseLinks = response.css('table.results a::attr(href)').extract()
		for href in caseLinks:
			# Skip if it's just a sorting link
			if 'inquiry-results' in href: continue
			# Make sure the case hasn't already been saved
			case_id = extractCaseId(href)
			try:
				self.cur.execute('SELECT EXISTS(SELECT 1 FROM rawcases WHERE case_id = %s)', (case_id,))
			except:
				self.logger.error('Failed to perform case_id lookup in rawcases')
				self.connectToDatabase()
				self.parseResults(self, response)
				return
			if not self.cur.fetchone()[0]:
				# If not GET the inquiry-details page
				yield response.follow(
					href,
					headers = {
						'Cookie': self.cookie
					},
					callback = self.saveCase
				)
			else:
				self.logger.info('Skipped %s (%s remaining)', case_id, len(self.crawler.engine.slot.scheduler))

		# Generate requests for additional results pages from the original one
		if not response.meta.get('Sub_Page') and len(caseLinks) > 0:
			pageLinks = set(response.css('span.pagelinks a::attr(href)').extract())
			for href in pageLinks:
				yield response.follow(
					href,
					headers = {
						'Cookie': self.cookie
					},
					meta = {
						'Sub_Page': True
					},
					callback = self.parseResults
				)

	# Insert case details page HTML into DB
	def saveCase(self, response, *args):
		# Redo request if response was not OK
		if response.status != 200:
			yield response.request
			return
		# Get case ID and execute query
		try:
			case_id = extractCaseId(response.url)
		except:
			self.logger.error('Failed to get case data for %s', response.url)
			return
		try:
			self.cur.execute('INSERT INTO rawcases (case_id, html) VALUES (%s, %s)', (case_id, response.text))
			self.conn.commit()
			self.logger.info('Saved %s (%s remaining)', case_id, len(self.crawler.engine.slot.scheduler))
		except:
			self.logger.error('Failed to insert row for %s', case_id)
			self.connectToDatabase()
			self.saveCase(self, response)
