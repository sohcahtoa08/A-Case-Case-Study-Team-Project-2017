from bs4 import BeautifulSoup
from attrnames import getAttributeName, getSectionName

def parseCase(html):
	# Import into BS
	soup = BeautifulSoup(html, 'html.parser')

	# Temporary KVP store
	data = {}
	# Full data list
	output = []

	# Get KVPs, headers, and separators
	rows = soup.find_all(['span', 'h5', 'h6', 'i', 'hr'])
	# Iterate thru page rows
	i = 0
	while i < len(rows):
		row = rows[i]

		# Save class list
		classes = row.attrs.get('class', [])

		if 'AltBodyWindowDcCivil' not in classes:
			# Field names
			if 'FirstColumnPrompt' in classes or 'Prompt' in classes:
				headerval = row.get_text()

				# Remove unnecessary end chars
				if headerval.endswith(':'): headerval = headerval[:-1]
				if headerval.endswith('?'): headerval = headerval[:-1]

				headerval = stripWhitespace(headerval)

				# Parse newer charge statue code format
				if headerval == 'Article' and stripWhitespace(rows[i+2].get_text()) == 'Sec:':
					# Build statute code from individual fields
					statuteCode = []
					for j in range(i+1, i+10, 2):
						if 'Value' in rows[j].attrs.get('class', []):
							statuteCode.append(stripWhitespace(rows[j].get_text()))
					# Append as KVP
					data['Statute Code'] = '.'.join(filter(None, statuteCode))
					# Skip to the next section after this
					i += 10
				# Parse jail and probation terms
				elif headerval in {'Jail Term', 'Suspended Term', 'UnSuspended Term', 'Probation', 'Supervised', 'UnSupervised'}:
					# Generate interval string from yrs+mos+days+hrs fields
					yrs = stripWhitespace(rows[i+2].get_text()) or '0'
					mos = stripWhitespace(rows[i+4].get_text()) or '0'
					days = stripWhitespace(rows[i+6].get_text()) or '0'
					hrs = stripWhitespace(rows[i+8].get_text()) or '0'
					# Cleanup intervals
					if yrs.isdigit() and mos.isdigit():
						iyrs = int(yrs)
						imos = int(mos)
						if imos > 12:
							iyrs += imos // 12
							imos %= 12
							yrs = str(iyrs)
							mos = str(imos)
					interval = yrs + '-' + mos + ' ' + days + ' ' + hrs + ':00:00'
					# Append as KVP
					data[headerval] = interval
					# Skip to the next section after this
					i += 8
				# Parse newer event table format
				elif row.parent.name == 'th' and headerval == 'Event Type':
					# Get number of table columns
					headerVals = []
					for j in range(i, len(rows)):
						if rows[j].parent.name != 'th':
							break
						else:
							headerVals.append(stripWhitespace(rows[j].get_text()))
					# Get row values
					rowVals = []
					for k in range(i+len(headerVals), len(rows)):
						if not 'Value' in rows[k].attrs.get('class', []):
							i = k # Skip to the next section after this
							break
						else:
							rowVals.append(stripWhitespace(rows[k].get_text()))
					# Split and append events
					for l in range(0, len(rowVals), len(headerVals)):
						eventData = {x[0]: x[1] for x in zip(headerVals, rowVals[l:l+len(headerVals)])}
						output.append(eventData)
			# Values
			elif 'Value' in classes:
				dataval = stripWhitespace(row.get_text())
				if headerval and dataval != 'MONEY JUDGMENT': # The 'money judgement' header is useless
					data[headerval] = dataval
			# Headers and separators
			else:
				if 'InfoChargeStatement' not in classes:
					if row.name in {'hr', 'h5', 'h6', 'i'}:
						header = stripWhitespace(row.get_text())
						# Skip over the charge subheadings
						if not (header in {'Disposition', 'Jail', 'Probation', 'Fine', 'Community Work Service'} and row.name == 'i' and row.parent.name == 'left'):
							# Append the KVPs and reset the temporary dict
							if data:
								output.append(data)
								data = {}
							# Add the header to the data list
							if row.name != 'hr':
								if header:
									output.append(header)

		# Increment index
		i += 1

	# Append any remaining KVPs
	if data:
		output.append(data)
		data = {}

	return formatOutput(output)

def formatOutput(data):
	# Final output dict
	output = {}

	# Add case information header if necessary
	if len(data) > 0 and not isinstance(data[0], str):
		data.insert(0, 'Case Information')

	# Iterate thru data list
	for i in range(len(data)):
		# Check if item is a section header
		if isinstance(data[i], str):
			# Get proper attribute name
			header = getSectionName(data[i])
			# Make sure section is going to be stored
			if header:
				entries = []
				# Find KVP dicts corresponding to this header
				for j in range(i+1, len(data)):
					# Stop looking when we reach a different header
					if isinstance(data[j], str):`																																												`
						break
					# Get proper attribute names for fields
					attrMap = formatAttrs(data[j], data[i], header)
					# Save this dict if it hasn't been nullified
					if attrMap:
						entries.append(attrMap)
				# Add all the data we found to the master dict
				if output.get(header):
					output[header] += entries
				elif entries:
					output[header] = entries

	# Move attorneys listed under parties to attorneys
	parties = output.get('parties', [])
	j = 0
	while j < len(parties):
		partyType = parties[j].get('type')
		# Check if party type is an attorney
		if partyType and partyType.lower().startswith('attorney for '):
			# Remove the party from the parties list
			party = parties.pop(j)
			j -= 1
			# Set the type to the appropriate attorney type
			party['type'] = partyType[13:]
			# Create attorneys key if necessary
			if not output.get('attorneys'):
				output['attorneys'] = []
			# Append this attorney
			output['attorneys'].append(party)
		j += 1

	return output

def formatAttrs(data, section, header):
	# Formatted output dict
	d = {}

	# Iterate thru fields in input dict
	for field in data:
		# Get proper field names
		formattedName = getAttributeName(field)
		d[formattedName] = data[field]
		if data[field]:
			# Format heights
			if formattedName == 'height' and ('\'' in data[field] or '"' in data[field]):
				vals = data[field].replace('"', '\'').split('\'')
				d[formattedName] = str(int(vals[0] or 0) * 12 + int(vals[1] or 0))
			# Format sex
			elif formattedName == 'sex':
				d[formattedName] = data[field].upper()[0]
			# Format dates
			elif ('date' in formattedName or formattedName == 'dob'):
				vals = data[field].split('/')
				if len(vals) < 3:
					d[formattedName] = vals[0] + '/01/' + vals[1]
			# Format booleans
			elif formattedName in {'probable_cause', 'accident_contribution', 'property_damage', 'seatbelts_used', 'mandatory_court_appearance'}:
				d[formattedName] = data[field].lower() in {'y', 'yes'}
			# Format traffic accident injuries
			elif formattedName == 'injuries' and not data[field].isdigit():
				d[formattedName] = 0

	# Assign attorneys a type based on what section they're in
	if section.startswith('Attorney(s) for the '):
		if d.get('appearance_date') or (d.get('name') and 'attorney' in d.get('name').lower()):
			d['type'] = section[20:]
		# Discard party information in the attorney sections
		else:
			return None
	# Assign officers a type to indicate that they're officers
	elif header == 'parties' and ('Surety' in section or 'Bond' in section or 'Defendant' in section or 'Plaintiff' in section or 'Officer' in section):
		d['type'] = section.replace(' Information', '')

	return d

def stripWhitespace(s):
	return ' '.join(s.split())

# This is only for testing
if __name__ == '__main__': print(parseCase(open('test.html', 'r').read()))
