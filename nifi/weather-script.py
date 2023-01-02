from meteostat import Hourly
import datetime

start = datetime.datetime.today() - datetime.timedelta(days=7)
end = datetime.datetime(2099, 1, 1)

data = Hourly('12375', start, end)
data = data.fetch()
data['date'] = data.index.date
data['hour'] = data.index.hour
cols = data.columns.tolist()
cols = cols[-2:] + cols[:-2]
data = data[cols]
data.to_csv('/home/vagrant/nifi/input/weather-data.csv', index=False)