import re
import psycopg2

lineformat = re.compile(r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] ((\"(GET|POST) )(?P<url>.+)(http\/1\.1")) (?P<statuscode>\d{3}) (?P<bytessent>\d+) (["](?P<refferer>(\-)|(.+))["]) (["](?P<useragent>.+)["])""", re.IGNORECASE)
log_path = 'logs.log'
f = open(log_path, 'r')

#DATABASE CONNECT
try:
	conn = psycopg2.connect(
		dbname='logs',
		user='thedronit',
		password='12323212t',
		host='localhost'
	)
	conn.autocommit = True

	with conn.cursor() as cursor:
		cursor.execute(
			"""CREATE TABLE IF NOT EXISTS nginx(
				id serial PRIMARY KEY,
				ip varchar(100) NOT NULL,
				datetimestring varchar(300) NOT NULL,
				url varchar(3000) NOT NULL,
				bytessent varchar(100) NOT NULL,
				referrer varchar(1000) NOT NULL,
				useragent varchar(500) NOT NULL,
				status varchar(100) NOT NULL,
				method varchar(100) NOT NULL,
				UNIQUE (ip, datetimestring, url, status, method)
			);"""
		)

	with conn.cursor() as cursor:
		cursor.execute(
			"""CREATE TABLE IF NOT EXISTS nginx_source(
				id serial PRIMARY KEY,
				url varchar(3000) NOT NULL,
				ip varchar(100) NOT NULL,
				method varchar(100) NOT NULL,
				useragent varchar(500) NOT NULL,
				count_uniq_source bigint NOT NULL,
				UNIQUE (url, ip, method, useragent)
			);"""
		)

	#*INSERT nginx TABEL 
	try:
		with conn.cursor() as cursor:
			for line in f:
				data = re.search(lineformat, line)
				if data:
					datadict = data.groupdict() #ВРОДЕ ЕСТЬ ЕЩЕ ИНФА
					ip = datadict["ipaddress"]
					datetimestring = datadict["dateandtime"]
					url = datadict["url"]
					bytessent = datadict["bytessent"]
					referrer = datadict["refferer"]
					useragent = datadict["useragent"]
					status = datadict["statuscode"]
					method = data.group(6) 

					values = ({
						'ip':ip,
						'datetimestring': datetimestring,
						'url': url,
						'bytessent': bytessent,
						'referrer': referrer,
						'useragent': useragent,
						'status': status,
						'method': method
					})

					cursor.execute(
						f"""
						INSERT INTO nginx (ip, datetimestring, url, bytessent, referrer, useragent, status, method) 
						VALUES (
							%(ip)s,
							%(datetimestring)s,
							%(url)s,
							%(bytessent)s,
							%(referrer)s,
							%(useragent)s,
							%(status)s,
							%(method)s
						) ON CONFLICT DO NOTHING;
						""", values
					)
		print("[SUC] nginx tabel insert")
	except Exception as error:
		print(f"[FAIL] nginx tabel insert\n\nError:\n{error}")


#*INSERT nginx_source TABEL 
	try:
		#select all uniq query by url and method
		with conn.cursor() as cursor:
			cursor.execute(
				f"""
				SELECT COUNT(url), url, method FROM public.nginx
				GROUP BY url, method
				ORDER BY COUNT(url) DESC;
				""")
			uqic_source = cursor.fetchall()

			#select info all ip and ua group by url and method
			
			for query in uqic_source:
				with conn.cursor() as cursor:
					values = ({
						'url': query[1],
						'method': query[2]
					})

					cursor.execute(
						f"""						
						INSERT INTO nginx_source (url, method, ip, useragent, count_uniq_source)
						SELECT url, method, ip, useragent, COUNT('url') as count_uniq_source
						FROM public.nginx
						WHERE url=%(url)s AND method=%(method)s
						GROUP BY ip, useragent, url, method
						ORDER BY COUNT('url') DESC
						ON CONFLICT (url, ip, method, useragent) DO UPDATE
						SET count_uniq_source = excluded.count_uniq_source;
						""", values)
					#print(cursor.fetchall())
			print("[SUC] nginx_source tabel insert")
	except Exception as error:
		print(f"[FAIL] nginx_source tabel insert\n\nError:\n{error}")



except Exception as error:
	print('Can`t establish connection to database, error:\n\n', error)
	exit()
	
finally:
	if conn:
		conn.close()

'''
		print (ip, \
			datetimestring, \
			url, \
			bytessent, \
			referrer, \
			useragent, \
			status, \
			method)
'''
				



