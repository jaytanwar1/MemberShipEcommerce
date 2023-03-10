from crontab import CronTab

# Define a new cron tab
cron = CronTab(user='username_test1')

# Define a new cron job
job = cron.new(command='/path/to/python /path/to/pyspark/script.py')

# Schedule cron job to run hourly
job.setall('0 * * * *')

# Save the cron tab
cron.write()