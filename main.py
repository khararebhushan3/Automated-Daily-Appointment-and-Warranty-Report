import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from datetime import datetime, timedelta
import resource_files.sql_connector as sqlc
import resource_files.emails as email_conf

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

# ETL function
def data_fetch():
    print(sqlc.LMS_LDB_DATABASE)
    sql_script_ew = f"""
    WITH RankedVehicles AS (
        SELECT 
            client_details.vin,
            car_details.sold_from,
            car_details.warranty_type,
            car_details.warranty_start_date,
            car_details.warranty_end_date,
            ROW_NUMBER() OVER (
                PARTITION BY client_details.vin 
                ORDER BY car_details.warranty_end_date DESC
            ) AS row_num
        FROM 
            car_details
        JOIN 
            client_details 
            ON car_details.user_id = client_details.id
        WHERE 
            car_details.status = 0  -- Status filtering directly in JOIN query
    )
    SELECT 
        vin,
        sold_from,
        warranty_type,
        warranty_start_date,
        warranty_end_date
    FROM 
        RankedVehicles
    WHERE 
        row_num = 1;
    """

    # Construct the engine1 string
    engine1 = create_engine(f"{sqlc.LMG_LDB_CONNECTION}://{sqlc.LMG_LDB_USERNAME}:{sqlc.LMG_LDB_PASSWORD}@{sqlc.LMG_LDB_HOST}:{sqlc.LMG_LDB_PORT}/{sqlc.LMG_LDB_DATABASE}")

    # Execute the SQL script and read the data into a DataFrame
    df_ew = pd.read_sql(sql_script_ew, con=engine1,parse_dates= ['warranty_start_date', 'warranty_end_date'])

    # Construct the engine1 string
    engine2 = create_engine(f"{sqlc.LMS_LDB_CONNECTION}://{sqlc.LMS_LDB_USERNAME}:{sqlc.LMS_LDB_PASSWORD}@{sqlc.LMS_LDB_HOST}:{sqlc.LMS_LDB_PORT}/{sqlc.LMS_LDB_DATABASE}")


    sql_script_appoint = f"""
    select  cmp.name as Brand,l.name as Location,
    cc.vin,
    cm.registration_date,
    SUM(CASE WHEN vs.vehicle_reported = 1 THEN 1 ELSE 0 END) AS YesReported, 
    SUM(CASE WHEN vs.vehicle_reported = 0 THEN 1 ELSE 0 END) AS NotReported,
    (count(DISTINCT cc.vin) - (SUM(CASE WHEN vs.vehicle_reported = 1 THEN 1 ELSE 0 END) +  SUM(CASE WHEN vs.vehicle_reported = 0 THEN 1 ELSE 0 END))) AS NotUpdated
    from completed_call as cc 
     left join completed_call_details as ccd on ccd.completed_call_id = cc.id 
     left join vehicle_service as vs on vs.completed_call_id = cc.id 
     left join customer_master as cm on cm.vin = cc.vin 
     left join client_details as cd on cd.customer_master_id = cm.id 
     left join outcome as o on o.outcome_id = cc.sub_sub_disposition_id 
     left join users as u on u.id = cc.agent_id 
     left join campaign as c on c.id = cc.campaign_id 
     left join locations as l on l.id = cc.appointment_location
     left join companies as cmp on cmp.id = cc.brand_id 
     where sub_sub_disposition_id in (select outcome_id FROM outcome AS ot LEFT JOIN outcome_types AS ott ON ott.outcome_type_id = ot.outcome_type 
     WHERE ot.outcome='APPOINTMENT' 
     AND (ot.suboutcome='PICK-UP' OR ot.suboutcome='SELF VISIT' OR ot.suboutcome='SERVICE@HOME') 
     AND ot.sub_suboutcome='CONFIRMED') 
     AND cc.brand_id = 4 -- brand adjustment
	 AND cc.vin is not null 
     AND cc.appointment_location is not null  
     AND cc.appointment_date = CURRENT_DATE
     group by cmp.id,l.id,cc.vin,cc.appointment_date order by YesReported asc;
       """
    
    engine2 = create_engine(f"{sqlc.LMS_LDB_CONNECTION}://{sqlc.LMS_LDB_USERNAME}:{sqlc.LMS_LDB_PASSWORD}@{sqlc.LMS_LDB_HOST}:{sqlc.LMS_LDB_PORT}/{sqlc.LMS_LDB_DATABASE}")

    # Execute the SQL script and read the data into a DataFrame
    df_appoint = pd.read_sql(sql_script_appoint, con=engine2, parse_dates= ['registration_date'])
    
    # Perform a left join on the 'vin' column
    merged_df = df_appoint.merge(df_ew, how='left', on='vin')
    
    return merged_df    


# Adding Additional columns
def calculate_warranty_status(df):
    # Convert columns to datetime
    df['registration_date'] = pd.to_datetime(df['registration_date'], format='%d-%m-%Y', errors='coerce')
    df['warranty_end_date'] = pd.to_datetime(df['warranty_end_date'], format='%d-%m-%Y', errors='coerce')
    
    # Calculate the later of warranty_end_date or registration_date + 3 years
    df['three_years_after_reg'] = df['registration_date'] + pd.DateOffset(years=3)
    df['relevant_date'] = df[['warranty_end_date', 'three_years_after_reg']].max(axis=1)

    # Today's date for comparison
    today = pd.Timestamp.now().normalize()

    # Calculate expiration status
    def expiration_status(row):
        if pd.isna(row['relevant_date']):
            return 'Unknown'
        days_diff = (row['relevant_date'] - today).days
        if row['registration_date'] and (today - row['registration_date']).days > 6 * 365:
            return 'Not Eligible'
        elif days_diff > 90:
            return 'Expiring in more than 90 days'
        elif 0 <= days_diff <= 90:
            return 'Expiring in 90 days'
        elif -90 <= days_diff < 0:
            return 'Expired under 90 days'
        else:
            return 'Expired more than 90 days'

    df['expiration_status'] = df.apply(expiration_status, axis=1)

    # Determine eligibility for 4th, 5th, and 6th year warranties
    def eligibility(row):
        if pd.isna(row['registration_date']):
            return 'Unknown'
        age_years = (today - row['registration_date']).days / 365.25
        eligibility_list = []
        if age_years > 6 or row['expiration_status'] == 'Not Eligible':
            return 'Not Eligible'
        if age_years <= 4:
            eligibility_list.append('4th Year')
        elif age_years <= 5:
            eligibility_list.append('5th Year')
        else:  # age_years <= 6
            eligibility_list.append('6th Year')
        return ', '.join(eligibility_list) if eligibility_list else 'Not Eligible'

    df['additional_warranty_eligibility'] = df.apply(eligibility, axis=1)
    df.drop(['three_years_after_reg', 'relevant_date'],axis = 1,inplace=True)
    return df

# Generating Email
def send_email(df):
    # Get the current date and time
    current_datetime = datetime.now()
    ist_datetime = current_datetime + timedelta(hours=5, minutes=30)
    current_date_str = ist_datetime.strftime("%Y_%m_%d_%H_%M")
    
    # Create the filename with the current date
    filename = f"/tmp/landmark_appointments_with_details_{current_date_str}.csv"

    df.to_csv(filename,index=False)

    # Create a pivot table grouped by location with counts for each expiration status
    pivot_table = pd.pivot_table(
        df,
        index='Location',
        columns='expiration_status',
        aggfunc='size',
        fill_value=0
    )

    # Calculate the Grand Total column
    pivot_table['Grand Total'] = pivot_table.sum(axis=1)

    # Add a Grand Total row
    pivot_table.loc['Grand Total'] = pivot_table.sum(axis=0)

    # Generate HTML table
    html_table = pivot_table.to_html(
        border=1,
        classes='table table-striped table-bordered',
        justify='center'
    )

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = email_conf.username
    msg['To'] = ', '.join(email_conf.TO_EMAILS)
    msg['Cc'] = ', '.join(email_conf.CC_EMAILS)
    msg['Subject'] = f"Daily Appointment Opportunities - {current_date_str}"

    # Email body with the HTML table
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css">
        <style>
            table {{ margin: auto; }}
            th, td {{ text-align: center; padding: 8px; }}
        </style>
    </head>
    <body>
        <p>Hi everyone,</p>
        <p>Please check the report below for today's potential sales for extended warranty and appointments that are still under warranty.</p>
        <p>Attached is the data for incoming appointments.</p>
        <h2 style="text-align: center;">Warranty Expiration Report</h2>
        {html_table}
        <p>Regards,<br>Data Products Team<br>Landmark Transformation Team</p>
        <p><br><br>Note-This is an auto-generated email.</p>
    </body>
    </html>
    """
    msg.attach(MIMEText(html_body, 'html'))

    # Attach the CSV file to the email
    with open(filename, 'rb') as f:
        mime_base = MIMEBase('application', 'octet-stream')
        mime_base.set_payload(f.read())
        encoders.encode_base64(mime_base)
        mime_base.add_header('Content-Disposition', f'attachment; filename={os.path.basename(filename)}')
        msg.attach(mime_base)

    # Send the email via IceWarp SMTP server
    with smtplib.SMTP(email_conf.smtp_server, email_conf.smtp_port) as server:
        server.starttls()  # Use TLS if supported by the server
        server.login(email_conf.username, email_conf.password)
        server.sendmail(email_conf.username, email_conf.TO_EMAILS + email_conf.CC_EMAILS, msg.as_string())

    try:
        # Check if the file exists
        if os.path.exists(filename):
            # Delete the file
            os.remove(filename)
            print(f"{filename} has been deleted successfully.")
        else:
            print(f"The file {filename} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return None

def lambda_handler(event, context):
    send_email(calculate_warranty_status(data_fetch()))