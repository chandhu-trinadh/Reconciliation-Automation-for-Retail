from flask import Flask, render_template, request
from pymongo import MongoClient
import pandas as pd
import logging
from bson import ObjectId
import numpy as np

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
app = Flask(__name__)

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017')
db = client['nukkad_reports']
shops_collection = db['shops']
reports_collection = db['reports']

@app.route('/', methods=['GET', 'POST'])
def index():
    # Fetch shops from the database
    shops = list(shops_collection.find({'enable_arms_integration': 1}, {'shop_details.shop_name': 1, 'ns_cin': 1}))

    # Convert the shops list for rendering  
    shops_list = [(shop['_id'], f"{shop['shop_details']['shop_name']} - {shop['ns_cin']}") for shop in shops]
    
    if request.method == 'GET':
        # Render the index page with shop list
        return render_template('index.html', shops=shops_list)
    
    elif request.method == 'POST':
        # Extract data from the form
        shop_id = request.form.get('shop_id')
        from_date_picker = request.form.get('from_date_picker')
        to_date_picker = request.form.get('to_date_picker')
        
        # Log form data for debugging
        logger.debug("Form data: shop_id: %s, from_date_picker: %s, to_date_picker: %s", shop_id, from_date_picker, to_date_picker)
        
        # Handle file upload
        file = request.files.get('source_file')
        if file:
            # Read the CSV file into a Pandas DataFrame
            csv_df = pd.read_csv(file, dayfirst=True)
            
            # Convert the 'date' column to datetime using pd.to_datetime and specifying the date format
            csv_df['date'] = pd.to_datetime(csv_df['date'], format='%d-%m-%Y', errors='coerce')
            
            # Filter rows based on the provided date range
            csv_df = csv_df[(csv_df['date'] >= from_date_picker) & (csv_df['date'] <= to_date_picker)]
            
            # Convert the 'date' column to string format with the desired format
            #csv_df['date'] = csv_df['date'].dt.strftime('%d-%m-%Y')
            # Sort DataFrame by 'date' column in ascending order
            csv_df = csv_df.sort_values(by='date', ascending=True) 
            logger.debug("CSV DataFrame loaded and filtered.")
            logger.debug("Filtered CSV DataFrame:\n%s", csv_df)

            if csv_df.empty:
                logger.debug("No records found in the CSV file for the selected date range.")
                return render_template('index.html', shops=shops_list, message="No records found in the CSV file for the selected date range.")
        else:
            logger.debug("No CSV file uploaded.")
            return render_template('index.html', shops=shops_list, message="No CSV file uploaded.")
        
        # Convert date picker inputs to datetime objects using `pd.to_datetime` with `dayfirst=True`
        from_date = pd.to_datetime(from_date_picker, format='%Y-%m-%d')
        to_date = pd.to_datetime(to_date_picker, format='%Y-%m-%d')
        
        # Log date range for debugging
        logger.debug("Date range: from_date: %s, to_date: %s", from_date, to_date)

        # Convert timestamps to the format of dd-mm-yyyy using strftime()
        from_date_formatted = from_date.strftime('%d-%m-%Y')
        to_date_formatted = to_date.strftime('%d-%m-%Y')

        # Query the reports collection for data related to the shop_id and date range
        reports_query = {
            'store_id': shop_id,
            'date': {
                '$gte': from_date_formatted,
                '$lte': to_date_formatted
            }
        }
        
        projection_fields = {
            'date': 1,
            'order_id': 1,
            'amount': 1,
            'tl_tax_amount': 1,
            'taxable_amount': 1,
        }
        
        # Fetch data from the reports collection
        reports_data_cursor = reports_collection.find(reports_query, projection_fields)
        
        # Convert the cursor to a DataFrame
        reports_df = pd.DataFrame(list(reports_data_cursor))
        # print(reports_df['date'])
        
        # Log the entire Reports DataFrame for debugging
        logger.debug("Reports DataFrame:\n%s", reports_df)

        # If no records are found
        if reports_df.empty:
            logger.debug("No records found for the given date range and shop.")
            return render_template('index.html', shops=shops, message="No records found for the given date range and shop.")   
        
        # Merge CSV data and reports data based on date and order_id
        differences = csv_df.merge(reports_df, on=['order_id'], how='inner')
        
        # Calculate differences between CSV data and reports data
        differences['Bill_Difference'] = differences['total'] - differences['amount']
        differences['Tax_Difference'] = differences['tax'] - differences['tl_tax_amount']
        differences['Net_Difference'] = differences['net'] - differences['taxable_amount']
        
        # Fetch the selected shop's ns_cin and shop_name from the database
        selected_shop = shops_collection.find_one({'_id': ObjectId(shop_id)}, {'shop_details.shop_name': 1, 'ns_cin': 1})
        
        if selected_shop:
            # Add ns_cin and shop_name columns to the differences DataFrame
            differences['shop_name'] = selected_shop['shop_details']['shop_name']
            differences['ns_cin'] = selected_shop['ns_cin']

        # Reset index for both DataFrames before concatenation
        differences.reset_index(drop=True, inplace=True)
        csv_df.reset_index(drop=True, inplace=True)

        # Extracting the desired columns from the differences DataFrame
        selected_columns_df1 = differences[['shop_name', 'ns_cin', 'total', 'amount', 'net', 'taxable_amount', 'tax', 'tl_tax_amount', 'Bill_Difference', 'Tax_Difference', 'Net_Difference']]
        selected_columns_df2 = csv_df[['date', 'order_id']]

        # Concatenate selected_columns_df1 and selected_columns_df2 column-wise
        final_selected_df = pd.concat([selected_columns_df2, selected_columns_df1], axis=1)

        # Add a serial number column by resetting the index
        final_selected_df.reset_index(drop=True, inplace=True)

        # Rename the index column to 'SerialNumber' if you prefer a different name
        final_selected_df.rename_axis("SerialNumber", inplace=True,index=1)
        final_selected_df.reset_index(inplace=True)
        final_selected_df['SerialNumber'] = final_selected_df.index + 1  # Start with 1 for all rows
        final_selected_df.loc[final_selected_df.index == 1, 'SerialNumber'] += 0  # Increment by 1 where index is 0

        # Replace NaN values with empty strings
        # Replace NaN values with empty strings
        final_selected_df = final_selected_df.fillna(value='')

        # Round off float values to two decimal places
        final_selected_df = final_selected_df.round(2)

        # Calculate total for each column
        total_row = {
            'SerialNumber': 'Total',
            'total': round(final_selected_df['total'].sum(), 2),
            'amount': round(final_selected_df['amount'].sum(), 2),
            'net': round(final_selected_df['net'].sum(), 2),
            'taxable_amount': round(final_selected_df['taxable_amount'].sum(), 2),
            'tax': round(final_selected_df['tax'].sum(), 2),
            'tl_tax_amount': round(final_selected_df['tl_tax_amount'].sum(), 2),
            'Bill_Difference': round(final_selected_df['Bill_Difference'].sum(), 2),
            'Tax_Difference': round(final_selected_df['Tax_Difference'].sum(), 2),
            'Net_Difference': round(final_selected_df['Net_Difference'].sum(), 2)
        }
        # Function to replace empty strings with NaN
        total_df = pd.DataFrame(total_row, index=[0])

        # Append total row to DataFrame
        final_selected_df = pd.concat([final_selected_df, total_df], ignore_index=True)
       
        # Replace NaN and NaT values with empty strings
        final_selected_df = final_selected_df.fillna('')
        # Replace NaT values in the 'date' column with empty strings
        final_selected_df['date'] = final_selected_df['date'].apply(lambda x: '' if pd.isnull(x) else x)
   
        #logger.debug("Final DataFrame with totals:\n%s", final_selected_df)

        # Convert final_selected_df to dictionary for rendering
        data_to_render = final_selected_df.to_dict('records')

        # Render the result.html template with the data
        return render_template('result.html', differences=data_to_render)
    
    # Default rendering of the index.html template
    return render_template('index.html', shops=shops)


if __name__ == '__main__':
    app.run(debug=True)














