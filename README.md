# sql-query-classic-rr
Calculating Classic retention rate based on the data of events using SQL query only

# WebApp Retention rate calculation using data stored in BigQuery

In this article I would like to cover a Web Application use case of calculating Classic retention rate based on the data of events using SQL query only. 

First let’s define our task. Our case is not a classic mobile application (iOS, Android, etc), we are working with a web application that launches in the browser like a typical web-site. Since we are still some kind of application we needed to solve an event collecting issue. Solutions for our case become services Google analytics and Firebase. I will not cover in this article how we solved the event collecting issue, because I would like to focus on the Retention rate topic. 

Second, we need to create a data source. To calculate retention rate we needed to have all data of tracked events, this data was produced by GA + Firebase services and we set up the processing of data to the BigQuery storage. 

Now when you have a short intro about a case, let’s move forward and create our SQL query. 

NOTICE: If you are using other storages like AWS, Snowflake, etc, then some part of the SQL query might not work for you, but overall the logic can be used as base with some adaptations.

**SQL query**

SQL query consists of several common table expressions (CTE). Each CTE is like a step of preparing data for the final view. 

To clarify, the SQL query will use as a base created earlier a view (virtual table based on the result-set of an SQL statement). I will not cover the topic of transforming RAW event data to some kind of Data Mart in this article, if you have a question about this please feel free to leave a comment. 

...

**Read full article in my medium blog: https://medium.com/@mirlan.irokez/webapp-retention-rate-calculation-using-data-stored-in-bigquery-3a3ffcf13921**
