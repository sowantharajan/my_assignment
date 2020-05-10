from kafka import KafkaConsumer, TopicPartition
import time
import json
import os
import re

#env-variables:
kafka_server = os.environ["KAFKA_SERVER"]
kafka_topic = os.environ["KAFKA_TOPIC"]

consumer = KafkaConsumer(kafka_topic, auto_offset_reset='earliest', bootstrap_servers=kafka_server)
messages = []
users = []
f_countries = {}
f_gender = {"Male": 0, "Female": 0}
f_email_host_domains = {}
f_email_top_level_domains = {}
f_days = {}
f_months = {}


def consume_data(kafka_topic):
	with open("messages.txt", "w") as messages_file:
		while(True):
			print("")
			print("")
			print("--------------MESSAGE STARTED------------------------")
			print("")
			message_raw = next(consumer)
			message = json.loads(message_raw.value.decode("utf-8"))
			messages.append(message)
			# Write messages to messages.txt 
			messages_file.write(str(message))
			# Call to function n_unique_users to show the number of users
			n_unique_users(message)
			# Call to function frecuency_countries to show the appearances of the countries
			frecuency_countries(message)
			# Call to function gender to show the gender of the users
			gender(message)
			# Call to function email_domains to show the most used domain names
			email_domains(message)
			# Call to function date to show the most used common days and months
			date(message)

			
def is_valid_ip_format(ip):
    if not ip:
        return False

    a = ip.split('.')

    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True


def is_valid_date(date_value):
    try:
        datetime.datetime.strptime(date_value, '%d/%m/%Y')
    except ValueError:
        return False

    return True


def is_valid(row):
    valid_ip_format = is_valid_ip_format(row.get('ip_address'))
    valid_date = is_valid_date(row.get('date'))

    return valid_ip_format and valid_date


def clean_entry():
    def meth(row):
        # Uppercase first letter of the country
        # e.g. Germany, germany, GeRmany -> Germany
        country = row['country'].lower().capitalize()
        row['country'] = country
        return Row(**row)
    return meth			

def n_unique_users(message):
	# It has been considered the first_name and last_name together as unique identifier 
	user = {"first_name": message["first_name"], "last_name": message["last_name"]}
	if(user not in users):
		users.append(user)

	print("NUMBER OR UNIQUE USERS: " + str(len(users)))
		

def frecuency_countries(message):
	country = message["country"]
	if country in f_countries:
		f_countries[country] += 1
	else:
		f_countries[country] = 1

	most_represented_country = max(f_countries, key=f_countries.get)
	print("MOST REPRESENTED COUNTRY: " + most_represented_country + 
							", WITH " + str(f_countries[most_represented_country]) + " APPEARANCES")
	least_represented_country = min(f_countries, key=f_countries.get)
	print("LEAST REPRESENTED COUNTRY: " + least_represented_country +
							", WITH " + str(f_countries[least_represented_country]) + " APPEARANCES")


def gender(message):
	""" Keeps track of the percentage of users that belong to each gender
	"""
	gender = message["gender"]
	f_gender[gender] += 1
	
	print("MESSAGES FROM MALE USERS: " + str(f_gender["Male"]))
	print("MESSAGES FROM FEMALE USERS: " + str(f_gender["Female"]))


def email_domains(message):
	""" Keeps track of which are the most common host domains and top level domains
	"""
	email = message["email"]
	domain = re.search("@[\w.-]+", email).group()

	top_level_domain = domain.split(".")[-1]

	index_top_level_domain = domain.index("."+top_level_domain)
	host_domain = domain[1:index_top_level_domain]

	if(host_domain in f_email_host_domains):
		f_email_host_domains[host_domain] += 1
	else:
		f_email_host_domains[host_domain] = 1

	if(top_level_domain in f_email_top_level_domains):
		f_email_top_level_domains[top_level_domain] += 1
	else:
		f_email_top_level_domains[top_level_domain] = 1

	most_used_host_domain = max(f_email_host_domains, key=f_email_host_domains.get)
	print("MOST USED HOST DOMAIN: " + most_used_host_domain + 
							", WITH " + str(f_email_host_domains[most_used_host_domain]) + " APPEARANCES")
	most_used_top_level_domain = max(f_email_top_level_domains, key=f_email_top_level_domains.get)
	print("MOST USED TOP LEVEL DOMAIN: " + most_used_top_level_domain +
							", WITH " + str(f_email_top_level_domains[most_used_top_level_domain]) + " APPEARANCES")


def date(message):
	""" Keeps track of the periods of time with more messages
	"""
	date = message["date"].split("/")
	day = date[0]
	month = date[1]

	if day in f_days:
		f_days[day] += 1
	else:
		f_days[day] = 1

	if month in f_months:
		f_months[month] += 1
	else:
		f_months[month] = 1

	most_common_day = max(f_days, key=f_days.get)
	print("MOST COMMON DAY: " + most_common_day + 
							", WITH " + str(f_days[most_common_day]) + " APPEARANCES")
	most_common_month = max(f_months, key=f_months.get)
	print("MOST COMMON MONTH: " + most_common_month +
							", WITH " + str(f_months[most_common_month]) + " APPEARANCES")

consume_data(kafka_topic)
