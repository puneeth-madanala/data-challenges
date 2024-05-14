#Solution 1: CTE for Event Summaries
cte_query = """
WITH EventSummary AS (
    SELECT
        order_id,max(date_time) AS latest_date,min(date_time) AS earliest_date,        
        last(status) OVER (PARTITION BY order_id ORDER BY date_time) AS latest_status,
		first(status) OVER (PARTITION BY order_id ORDER BY date_time) AS earliest_status,
        count(event_id) AS event_count,sum(cost) AS total_cost        
    FROM events GROUP BY order_id,status,date_time )
SELECT
    order_id,earliest_status,latest_status,total_cost,event_count from EventSummary;"""

# Execute the cte_query by passing it to the conn object which already connected to DuckDB database
summary_df = conn.execute(cte_query).df()

# print the output from summary_df
print(summary_df)


#Solution 2: Window Function for Running Costs
total_running_query = """
WITH Evtdetails AS ( Select e.order_id,e.date_time,o.dealership_id,e.event_id,e.status,e.cost from events e 
    join orders o ON e.order_id = o.order_id
)
SELECT  date_time,dealership_id,order_id, event_id,cost,
    SUM(cost) OVER ( PARTITION BY dealership_id  ORDER BY date_time) AS running_total_cost
FROM Evtdetails Order by dealership_id, date_time; """

total_running_df = conn.execute(total_running_query).df()

print(total_running_df)

#Solution 3: Joining Tables and Analyzing Data:
all_tables_joined_query = """
WITH JoinedData AS (
    Select
        t.technician_id, t.name AS technician_name, t.expertise,
        o.order_id,o.dealership_id,o.status AS order_status,o.total_cost AS order_total_cost,o.technician,o.order_date,
        d.location,d.manager,d.name AS dealership_name,
        e.event_id,e.status AS event_status,e.cost AS event_cost,e.date_time AS event_date_time 
    from orders o LEFT JOIN events e ON o.order_id = e.order_id
    left join technicians t ON o.technician = t.name
    left join dealerships d ON o.dealership_id = d.dealership_id
)
"""

event_summary_query = """
WITH EvtSummary AS (
    SELECT
        order_id,max(date_time) AS latest_date,min(date_time) AS earliest_date,        
        last(status) OVER (PARTITION BY order_id ORDER BY date_time) AS latest_status,
		first(status) OVER (PARTITION BY order_id ORDER BY date_time) AS earliest_status,
        count(event_id) AS event_count,sum(cost) AS total_cost        
    FROM events GROUP BY order_id,status,date_time
)
SELECT
    jd.order_id, jd.order_status,jd.order_total_cost,jd.technician,jd.order_date,
    es.earliest_status,es.latest_status, es.total_cost AS event_total_cost,es.event_count,
    jd.technician_name, jd.expertise, jd.dealership_name, jd.location, jd.manager from (Select
        t.technician_id, t.name AS technician_name, t.expertise,
        o.order_id,o.dealership_id,o.status AS order_status,o.total_cost AS order_total_cost,o.technician,o.order_date,
        d.location,d.manager,d.name AS dealership_name,
        e.event_id,e.status AS event_status,e.cost AS event_cost,e.date_time AS event_date_time 
    from orders o LEFT JOIN events e ON o.order_id = e.order_id
    left join technicians t ON o.technician = t.name
    left join dealerships d ON o.dealership_id = d.dealership_id) jd
 left join EvtSummary es ON jd.order_id = es.order_id ORDER BY jd.order_id;
"""

technician_performance_query = """
WITH TechnicianPerformance AS (select  technician_name, COUNT(DISTINCT o.order_id) AS completed_orders, SUM(e.cost) AS total_cost
    from  (Select
        t.technician_id, t.name AS technician_name, t.expertise,
        o.order_id,o.dealership_id,o.status AS order_status,o.total_cost AS order_total_cost,o.technician,o.order_date,
        d.location,d.manager,d.name AS dealership_name,
        e.event_id,e.status AS event_status,e.cost AS event_cost,e.date_time AS event_date_time 
    from orders o LEFT JOIN events e ON o.order_id = e.order_id
    left join technicians t ON o.technician = t.name
    left join dealerships d ON o.dealership_id = d.dealership_id) jd  left join orders o ON jd.order_id = o.order_id left join events e ON jd.order_id = e.order_id AND e.status = 'Completed' WHERE o.status = 'Completed' GROUP BY  technician_name ORDER BY total_cost DESC )
select technician_name,  completed_orders,   total_cost from TechnicianPerformance  LIMIT 5;
"""

dealership_performance_query = """
WITH performance_of_dealership AS (
    Select dealership_name,COUNT(DISTINCT o.order_id) AS completed_orders,SUM(e.cost) AS total_cost from  (Select
        t.technician_id, t.name AS technician_name, t.expertise,
        o.order_id,o.dealership_id,o.status AS order_status,o.total_cost AS order_total_cost,o.technician,o.order_date,
        d.location,d.manager,d.name AS dealership_name,
        e.event_id,e.status AS event_status,e.cost AS event_cost,e.date_time AS event_date_time 
    from orders o LEFT JOIN events e ON o.order_id = e.order_id
    left join technicians t ON o.technician = t.name
    left join dealerships d ON o.dealership_id = d.dealership_id) jd
    left join orders o ON jd.order_id = o.order_id
    left join events e ON jd.order_id = e.order_id AND e.status = 'Completed'
    where o.status = 'Completed' group by dealership_name order by total_cost desc)
Select dealership_name, completed_orders, total_cost from performance_of_dealership limit 5;
"""


summary_df = conn.execute(event_summary_query).df()
print(summary_df)
technician_perf_df = conn.execute(technician_performance_query).df()
print(technician_perf_df)
dealership_perf_df = conn.execute(dealership_performance_query).df()
print(dealership_perf_df)



#Bonus Task
#To create visualizations using the results of the analysis, we can use Python libraries such as pandas for data manipulation and matplotlib or seaborn.#

import pandas as pd #importing Pandas library
import matplotlib.pyplot as plt # importing matplotlib library and then pyplot method from it.
import seaborn as sns #importing seaborn library


sns.set(style="darkgrid") #background with drak grids for better viewing, v can choose whitegrid as well.

plt.figure(figsize=(10, 5))
sns.lineplot(data=summary_df, x='order_date', y='event_total_cost', hue='order_status', marker='o')
plt.title('Cost Trends Over Time by Order Status')# title of the plot
plt.xlabel('order_date')# x-axis as Total Cost
plt.ylabel('event_total_costt')# Y-axis as Total Event Cost
plt.legend(title='order_status') # title of the Legend (this is displayed within the graph)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


plt.figure(figsize=(10, 5))
sns.barplot(data=technician_perf_df, x='total_cost', y='technician_name', palette='viridis')
plt.title('Top Performing Technicians by Total Cost') # title of the plot
plt.xlabel('total_cost') # x-axis as Total Cost
plt.ylabel('technician_name') # Y-axis as Techician name
plt.tight_layout()
plt.show()


plt.figure(figsize=(10, 5))
sns.barplot(data=dealership_perf_df, x='total_cost', y='dealership_name', palette='magma')
plt.title('Top Performing Dealerships by Total Cost')# title of the plot
plt.xlabel('total_cost')# x-axis as Total Cost
plt.ylabel('dealership_name')# Y-axis as Dealership name
plt.tight_layout()
plt.show()
