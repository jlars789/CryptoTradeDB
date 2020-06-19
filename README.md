# CryptoTradeDB

The database handler for the CryptoTrader project. 

This Database Handler makes a call to the coincap.io API to retrieve exchange rates of all tradeable currencies on the Coinbase Website.
It will make 6 calls per hour, then enter the average of all to the Hourly table.

Every day at 12:00 AM UTC, it will find the average of the last 24 entries into the Hourly table, then make an entry into the Daily table.
