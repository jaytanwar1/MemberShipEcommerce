import requests
import pandas as pd
import matplotlib.pyplot as plt

# Fetch data from COVID19API
url = 'https://api.covid19api.com/total/dayone/country/singapore'
response = requests.get(url)
data = response.json()

# Convert data to Pandas DataFrame
df = pd.DataFrame(data)
df['Date'] = pd.to_datetime(df['Date']).dt.date
df.set_index('Date', inplace=True)

# Generate line graph
plt.plot(df['Confirmed'])
plt.title('Number of COVID19 Cases in Singapore')
plt.xlabel('Date')
plt.ylabel('Number of Cases')
plt.show()