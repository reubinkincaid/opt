import pandas as pd
import os
from glob import glob
import datetime

def build_options_master_database(
    base_dir='options_data',
    output_dir='options_db',
    master_file='options_master.parquet',
    run_type_filter='evening',  # Added parameter to filter by run_type
    incremental=True
):
    """
    Build or update a master database from raw options parquet files in nested directories.
    
    Args:
        base_dir (str): Base directory containing the nested folder structure
        output_dir (str): Directory to store the master database
        master_file (str): Filename for the master database
        run_type_filter (str): Filter to only include specific run_type ('evening', 'morning', or None for all)
        incremental (bool): If True, update existing database; if False, rebuild from scratch
        
    Returns:
        pd.DataFrame: The master database
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    master_path = os.path.join(output_dir, master_file)
    
    # Find all raw_options.parquet files recursively
    all_files = []
    print(f"Searching for parquet files in {base_dir}...")
    
    for root, _, files in os.walk(base_dir):
        if 'raw_options.parquet' in files:
            # Check if we're filtering by run_type
            if run_type_filter:
                # Check if the path contains the run_type we want
                path_parts = root.split(os.path.sep)
                # The run_type is typically the last directory before the file
                if path_parts and path_parts[-1] == run_type_filter:
                    file_path = os.path.join(root, 'raw_options.parquet')
                    date_info = '/'.join(path_parts[-5:]) if len(path_parts) >= 5 else root
                    all_files.append((file_path, date_info))
            else:
                # If not filtering, include all files
                file_path = os.path.join(root, 'raw_options.parquet')
                path_parts = root.split(os.path.sep)
                date_info = '/'.join(path_parts[-5:]) if len(path_parts) >= 5 else root
                all_files.append((file_path, date_info))
    
    run_type_msg = f" for run_type '{run_type_filter}'" if run_type_filter else ""
    print(f"Found {len(all_files)} raw options files{run_type_msg}")
    
    # Check if we should update existing database
    if incremental and os.path.exists(master_path):
        print(f"Loading existing master database: {master_path}")
        try:
            master_db = pd.read_parquet(master_path)
            print(f"Loaded {len(master_db):,} rows from existing database")
            
            # Track existing data combinations to avoid duplicates
            # Create a unique identifier for each trading_date + run_type combination
            # Create a more robust set of keys for identifying existing data
            existing_dates = set()
            if 'trading_date' in master_db.columns and 'run_type' in master_db.columns:
                # Try using trading_date as string + run_type
                try:
                    existing_dates_1 = set(
                        master_db['trading_date'].astype(str) + '_' + master_db['run_type']
                    )
                    existing_dates.update(existing_dates_1)
                    print(f"Created {len(existing_dates_1)} primary identifier keys")
                except Exception as e:
                    print(f"Warning: Could not create primary identifier keys: {str(e)}")
                
                # Also use the folder path as an alternative identifier
                # Most reliable for your nested folder structure
                if 'path_info' not in master_db.columns:
                    master_db['path_info'] = 'unknown'  # Default for existing data
                existing_dates.update(set(master_db['path_info']))
            else:
                print("Warning: Existing database missing key columns for duplicate detection")
            
            # Process all files and check for duplicates
            new_data_frames = []
            skipped_files = 0
            
            for file_path, date_info in all_files:
                try:
                    print(f"Checking {date_info}...")
                    df = pd.read_parquet(file_path)
                    
                    # Skip if empty
                    if df.empty:
                        print(f"Skipping empty file: {date_info}")
                        continue
                        
                    # Apply run_type filter at the dataframe level as a failsafe
                    if run_type_filter and 'run_type' in df.columns:
                        df = df[df['run_type'] == run_type_filter]
                        if df.empty:
                            print(f"Skipping file after filtering: {date_info}")
                            continue
                    
                    # Extract the folder path for more reliable duplication checking
                    path_info = date_info

                    # Check if this data already exists using multiple methods
                    is_duplicate = False

                    try:
                        # Method 1: Check using trading_date + run_type
                        if 'trading_date' in df.columns and 'run_type' in df.columns:
                            sample_date = str(df['trading_date'].iloc[0])
                            sample_run_type = df['run_type'].iloc[0]
                            sample_key = f"{sample_date}_{sample_run_type}"
                            
                            if sample_key in existing_dates:
                                print(f"Skipping already imported data (by date): {date_info}")
                                is_duplicate = True
                    except Exception as e:
                        print(f"Warning: Could not check duplicate by date: {str(e)}")

                    # Method 2: Check using folder path
                    if not is_duplicate and path_info in existing_dates:
                        print(f"Skipping already imported data (by path): {date_info}")
                        is_duplicate = True

                    # Method 3: For new entries, extract year/month/day from path
                    # This is a fallback method for when direct comparison fails
                    if not is_duplicate:
                        try:
                            path_parts = date_info.split('/')
                            if len(path_parts) >= 5:
                                year, month, _, day, run_type = path_parts[-5:]
                                date_str = f"{year}-{month}-{day}"
                                path_key = f"{date_str}_{run_type}"
                                
                                if path_key in existing_dates:
                                    print(f"Skipping already imported data (by path date): {date_info}")
                                    is_duplicate = True
                        except Exception as e:
                            print(f"Warning: Could not extract date from path: {str(e)}")

                    if is_duplicate:
                        skipped_files += 1
                        continue

                    # Add path_info to the dataframe for future duplicate detection
                    df['path_info'] = path_info

                    print(f"Adding new data from: {date_info}")
                    new_data_frames.append(df)
                        
                except Exception as e:
                    print(f"Error processing {date_info}: {str(e)}")
            
            # If we have new data, append it
            if new_data_frames:
                print(f"Appending {len(new_data_frames)} new files to master database")
                try:
                    # First, concatenate the new data frames
                    new_data = pd.concat(new_data_frames, ignore_index=True)
                    
                    # Ensure datatypes match between new_data and master_db
                    # For columns that exist in both dataframes
                    common_columns = set(master_db.columns).intersection(set(new_data.columns))
                    
                    # Convert date columns to strings in both dataframes before concat
                    date_columns = ['trading_date', 'expiration']
                    for col in date_columns:
                        if col in common_columns:
                            # Convert to string in both dataframes to ensure compatibility
                            master_db[col] = master_db[col].astype(str)
                            new_data[col] = new_data[col].astype(str) 
                    
                    # Apply the same conversions to other key columns if needed
                    for col in common_columns:
                        if master_db[col].dtype != new_data[col].dtype:
                            print(f"Converting column {col} to ensure type compatibility")
                            # Try to convert to the same type as master_db
                            try:
                                new_data[col] = new_data[col].astype(master_db[col].dtype)
                            except:
                                # If that fails, convert both to string
                                print(f"Converting {col} to string in both dataframes")
                                master_db[col] = master_db[col].astype(str)
                                new_data[col] = new_data[col].astype(str)
                                
                    # Now concatenate with the master database
                    master_db = pd.concat([master_db, new_data], ignore_index=True)
                    print(f"Master database now has {len(master_db):,} rows")
                    
                    # Re-convert date columns to datetime
                    for col in date_columns:
                        if col in master_db.columns:
                            try:
                                master_db[col] = pd.to_datetime(master_db[col])
                                print(f"Converted {col} to datetime")
                            except Exception as e:
                                print(f"Warning: Could not convert {col} to datetime: {str(e)}")
                    
                    # Recalculate derived columns
                    try:
                        # Calculate days-to-expiration
                        if 'trading_date' in master_db.columns and 'expiration' in master_db.columns:
                            master_db['days_to_expiry'] = (
                                pd.to_datetime(master_db['expiration']) - 
                                pd.to_datetime(master_db['trading_date'])
                            ).dt.days
                            
                        # Calculate moneyness
                        if 'strike' in master_db.columns and 'underlying_price' in master_db.columns:
                            master_db['moneyness'] = master_db['strike'] / master_db['underlying_price']
                    except Exception as e:
                        print(f"Warning: Error calculating derived columns: {str(e)}")
                    
                    # Save updated database
                    master_db.to_parquet(master_path)
                    print(f"Saved updated master database to {master_path}")
                except Exception as e:
                    print(f"Error updating existing database: {str(e)}")
                    print("Falling back to rebuilding database from scratch")
                    incremental = False
            else:
                print(f"No new data to add. Skipped {skipped_files} existing files.")
            
            return master_db
            
        except Exception as e:
            print(f"Error updating existing database: {str(e)}")
            print("Falling back to rebuilding database from scratch")
            incremental = False
    
    # Build new database from scratch if needed
    if not incremental or not os.path.exists(master_path):
        print("Building new master database from scratch")
        
        all_dfs = []
        processed = 0
        errors = 0
        
        for file_path, date_info in all_files:
            try:
                print(f"Processing {date_info}...")
                df = pd.read_parquet(file_path)
                
                # Skip if empty
                if df.empty:
                    print(f"Skipping empty file: {date_info}")
                    continue
                
                # Apply run_type filter again at the dataframe level as a failsafe
                if run_type_filter and 'run_type' in df.columns:
                    df = df[df['run_type'] == run_type_filter]
                    if df.empty:
                        print(f"Skipping file after filtering: {date_info}")
                        continue
                
                # Ensure essential columns exist
                if 'trading_date' not in df.columns:
                    # Try to extract date from path
                    parts = date_info.split('/')
                    if len(parts) >= 3:
                        year, month, day = parts[-5], parts[-4], parts[-2]
                        df['trading_date'] = f"{year}-{month}-{day}"
                
                all_dfs.append(df)
                processed += 1
                
                # Periodically save progress for very large datasets
                if processed % 50 == 0:
                    print(f"Processed {processed}/{len(all_files)} files...")
                
            except Exception as e:
                print(f"Error processing {date_info}: {str(e)}")
                errors += 1
        
        # Combine all dataframes
        if all_dfs:
            master_db = pd.concat(all_dfs, ignore_index=True)
            
            # Convert date columns to appropriate types
            date_columns = ['trading_date', 'expiration']
            for col in date_columns:
                if col in master_db.columns:
                    try:
                        master_db[col] = pd.to_datetime(master_db[col])
                    except:
                        print(f"Warning: Could not convert {col} to datetime")
            
            # Add derived columns for analysis
            try:
                # Calculate days-to-expiration for each option at time of recording
                if 'trading_date' in master_db.columns and 'expiration' in master_db.columns:
                    master_db['days_to_expiry'] = (
                        pd.to_datetime(master_db['expiration']) - 
                        pd.to_datetime(master_db['trading_date'])
                    ).dt.days
                
                # Calculate moneyness
                if 'strike' in master_db.columns and 'underlying_price' in master_db.columns:
                    master_db['moneyness'] = master_db['strike'] / master_db['underlying_price']
            except Exception as e:
                print(f"Warning: Error calculating derived columns: {str(e)}")
            
            # Save the master database
            master_db.to_parquet(master_path)
            print(f"Successfully built master database with {len(master_db):,} rows")
            print(f"Saved to {master_path}")
            print(f"Processed {processed} files with {errors} errors")
            
            return master_db
        else:
            print(f"No valid data found in {len(all_files)} files")
            return None

# Add this part to execute when the script is run directly
if __name__ == "__main__":
    # Adjust these paths to match your system
    master_df = build_options_master_database(
        base_dir='options_data',     # Path to your nested options data
        output_dir='/Users/studio/Documents/options_database',     # Where to save the master database
        run_type_filter='evening',   # Only include evening data
        incremental=True             # Update existing or create new
    )