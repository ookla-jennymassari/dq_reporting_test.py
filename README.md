DQ Reporting project

Set Up:

* Create gitignore and add env file
* Create .env file and add the credentials
* Installs dependencies


Wave 01:

- Retrieve all the data needed for the reporting.

Wave 02:

- Brainstorm the logic to filter the data need for visualization.

Wave 03:

- Create visualization (plot and table)for:
    
    * DL Network Category - plot/ sheet
    * Data Network Category (Download, Upload, LDRs) - sheet
    * Download Network Technology - plot/ sheet
    * M2M VoLTE/VoNR/EPS Fallback - plot/ sheet
    * Mobile-to-Mobile Call Block - plot/ sheet
    * Mobile-to-Mobile Call Drop - plot/ sheet

    * Download Throughput - plot/ sheet
    * Upload Throughput - plot/ sheet
    * Download Access Success - plot/ sheet
    * Download Task Success - plot/ sheet

Wave 04:

- Visualization with streamlit
- Visualization with HTML

Wave 05:

- Query from Athena instead of Postgres

Wave 06:

- Automate the email 


Refactor ideas:

- Create functions to get the data by csid curr or comp reduncing having two queries for the same logic.
- Use a variable to filter by country UK or US. Instead of having to unselect the logic for each country

Future ideas:

-How to better query the data. Avoiding joins.

-Incorporate python insights. automate text of the report
add comments

- Maybe change the methodoly to calculate LTE/ Volte: test level data

To do:
- Table that has network type id comparing with network type name

plotly express library - take a look 
- test UK market
- test Boston, NY 