# ==============================================================================
# IMPORTS - Economic Indicators Extractor Bot
# ==============================================================================
from datetime import datetime, timedelta
from queries import (TREASURY_YIELD, FEDERAL_FUNDS_RATE, CPI, RETAIL_SALES, INFLATION, 
                    DURABLES, UNEMPLOYMENT, NONFARM_PAYROLL, REAL_GDP, REAL_GDP_PC, 
                    EARNINGS, DIVIDENDS, STOCK_SPLITS, EARNINGS_CALENDAR)
from uuid import uuid4
import io, json, csv, psycopg2, time as time_module
from decimal import Decimal

# ==============================================================================
# UTILITY FUNCTIONS - Data Processing Helpers
# ==============================================================================

# Fill missing values in economic indicator time series using forward/backward fill
def forward_backward_fill_indicator(cursor, table_name, column_name, interval):
    """Forward and backward fill missing values in economic indicator data"""
    cursor.execute(f'SELECT "{interval}_economic_indicator_date", "{interval}_economic_indicator_{column_name}" FROM {table_name} ORDER BY "{interval}_economic_indicator_date"')
    records = cursor.fetchall()
    if not records:
        return
    
    # Find first valid value for backward fill
    first_valid_value = next((r[1] for r in records if r[1] is not None), None)
    if first_valid_value is None:
        return
    
    # Prepare updates for missing values
    updates = []
    last_known_value = first_valid_value
    for record in records:
        if record[1] is not None:
            last_known_value = record[1]
        else:
            updates.append((last_known_value, record[0]))
    
    # Apply updates if any missing values found
    if updates:
        cursor.executemany(f'UPDATE {table_name} SET "{interval}_economic_indicator_{column_name}" = %s WHERE "{interval}_economic_indicator_date" = %s AND "{interval}_economic_indicator_{column_name}" IS NULL', updates)

# Parse date string into date object, handles various formats and invalid inputs
def parse_date(date_str, fmt="%Y-%m-%d"):
    """Parse date string with error handling"""
    if not date_str or date_str in ["N/A", "", None]:
        return None
    try:
        return datetime.strptime(date_str, fmt).date()
    except Exception:
        return None

# Extract quarter (Q1, Q2, Q3, Q4) from date string
def get_quarter_from_date(date_str):
    """Extract quarter from date string"""
    if not date_str:
        return None
    try:
        month = datetime.strptime(date_str, '%Y-%m-%d').month
        return f"Q{(month-1)//3 + 1}"
    except:
        return None

# Convert split factor to readable string format (e.g., "2-for-1", "Reverse 3-for-1")
def compute_split_ratio_str(factor):
    """Convert split factor to readable string format"""
    if factor is None or factor == '':
        return None
    try:
        f = Decimal(str(factor))
        if f == 1:
            return None
        if f > 1:
            return f"{int(f.quantize(Decimal(1)))}-for-1"
        else:
            return f"Reverse {int((Decimal(1) / f).quantize(Decimal(1)))}-for-1"
    except Exception:
        return None

# Sanitize data for being inserted into the notes column
def sanitize_for_text(obj):
    """Sanitize data for being inserted into the notes column"""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_text(x) for x in obj]
    if isinstance(obj, dict):
        return {k: sanitize_for_text(v) for k, v in obj.items()}
    return obj

# ==============================================================================
# ECONOMIC INDICATORS PROCESSING - Fetch and Store Economic Data
# ==============================================================================

# Main function to process economic indicators from various APIs
def process_economic_indicators(cursor, interval_list, items, shown_warnings, already_reported):
    """
    Extract, transform and load economic indicators data
    Handles: GDP, Treasury yields, Federal funds rate, CPI, inflation, unemployment, etc.
    """
    print("\n[EXTRACTION] Starting economic indicators processing...")
    
    indicators_already_available = []
    no_values_by_interval = {}
    total_indicators_inserted = 0
    
    # Mapping of indicators to their API functions and column names
    indicator_map = {
        'real_GDP_interval_list': ('RGDPBUSD_value', REAL_GDP),
        'real_GDP_pc_interval_list': ('RGDPCBUSD_value', lambda: REAL_GDP_PC()),
        'Treasury_yield_interval_list': ('TCMRP_value', TREASURY_YIELD),
        'Federal_funds_interval_list': ('EFFRP_value', FEDERAL_FUNDS_RATE),
        'CPI_interval_list': ('CPIAUC_value', CPI),
        'inflation_interval_list': ('ICPP_value', lambda: INFLATION()),
        'retail_sales_interval_list': ('ARSRTMUSD_value', lambda: RETAIL_SALES()),
        'durables_interval_list': ('MNODGMUSD_value', lambda: DURABLES()),
        'unemployment_interval_list': ('URP_value', lambda: UNEMPLOYMENT()),
        'nonfarm_payrolls_interval_list': ('TNPTP_value', lambda: NONFARM_PAYROLL())
    }
    
    # Database column prefixes for different time intervals
    prefix_map = {'daily': 'D', 'weekly': 'W', 'monthly': 'M', 'quarterly': 'Q', 'semiannual': 'SA', 'annual': 'A'}
    
    # Process each time interval (daily, weekly, monthly, etc.)
    for list_ in interval_list:
        monitoring_count, follow_up_count = 0, 0
        
        # Process each indicator for current interval
        for item in items:
            indicator, interval_ = item
            
            # Skip if indicator doesn't support this interval
            if list_ not in interval_:
                if list_ not in no_values_by_interval:
                    no_values_by_interval[list_] = []
                key = (indicator, list_)
                if key not in already_reported:
                    no_values_by_interval[list_].append(indicator.replace('_interval_list', ''))
                    already_reported.add(key)
                continue
                
            # Skip if indicator not in our mapping
            if indicator not in indicator_map:
                continue
                
            column_suffix, data_func = indicator_map[indicator]
            column_name = prefix_map[list_] + column_suffix
            
            # Get the last date we have data for this indicator
            cursor.execute(f'''SELECT MAX("{list_}_economic_indicator_date") 
                             FROM "dyGEO".{list_}_economic_indicator_log 
                             WHERE "{list_}_economic_indicator_{column_name}" IS NOT NULL''')
            prev_date = cursor.fetchone()[0] or datetime.strptime('2000-01-01', '%Y-%m-%d').date()
            
            # Get next ID for new records
            cursor.execute(f'SELECT MAX("{list_}_economic_indicator_ID") FROM "dyGEO".{list_}_economic_indicator_log')
            id_ = cursor.fetchone()[0] or 0
            
            # Fetch data from API
            try:
                data = data_func(list_) if 'real_GDP' in indicator or 'Treasury' in indicator or 'Federal' in indicator or 'CPI' in indicator else data_func()
            except TypeError:
                data = data_func()
            
            # Handle different API response formats
            if isinstance(data, dict) and 'data' in data:
                # New format: {"status": ..., "data": [...]}
                data_list = data['data']
            elif isinstance(data, dict) and 'data' not in data:
                # Old format: direct data structure
                data_list = data.get('data', data)  # Try 'data' key, fallback to entire dict
            else:
                # Fallback
                data_list = data
                
            # Filter for new data only (after our last date)
            new_data = [entry for entry in data_list if datetime.strptime(entry['date'], '%Y-%m-%d').date() > prev_date]
            new_data.reverse()
            
            # Skip if no new data available
            if not new_data:
                key = (indicator, list_, str(prev_date))
                if key not in already_reported:
                    indicators_already_available.append(f"{indicator} ({list_}) till {prev_date}")
                    already_reported.add(key)
                continue
                
            # Process and organize data by date
            date_data = {}
            for entry in new_data:
                date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
                
                # Special handling for weekly federal funds rate (adjust by 5 days)
                if column_name == 'WEFFRP_value':
                    date = date - timedelta(days=5)
                    if date == prev_date:
                        continue
                        
                value_ = entry['value']
                if value_ in ['.', '']:
                    continue
                    
                # Convert value to appropriate numeric type
                try:
                    if column_name in ["MTNPTP_value", "TNPTP_value", "DTCMRP_value"] or "TNPTP_value" in column_name or "DTCMRP_value" in column_name:
                        value = int(float(value_))
                    else:
                        value = float(value_)
                except (ValueError, TypeError):
                    warning_msg = f"Could not convert value '{value_}' for {column_name}"
                    if warning_msg not in shown_warnings:
                        print(f"[WARNING] {warning_msg}")
                        shown_warnings.add(warning_msg)
                    continue
                    
                if date not in date_data:
                    date_data[date] = {}
                date_data[date][column_name] = value
                
                # Progress monitoring
                monitoring_count += 1
                if monitoring_count == follow_up_count + 1000:
                    print(f"[INFO] Processed {monitoring_count} records for {indicator} ({list_})")
                    follow_up_count += 1000
            
            # Insert/update data if we have any
            if date_data:
                # Check which dates already exist in database
                dates_list = list(date_data.keys())
                cursor.execute(f'''SELECT "{list_}_economic_indicator_date", "{list_}_economic_indicator_PK"
                                 FROM "dyGEO".{list_}_economic_indicator_log 
                                 WHERE "{list_}_economic_indicator_date" = ANY(%s)''', (dates_list,))
                existing_dates = dict(cursor.fetchall())
                
                # Separate updates vs inserts
                updates, inserts = [], []
                for date, values in date_data.items():
                    id_ += 1
                    if date in existing_dates:
                        # Update existing record
                        pk = existing_dates[date]
                        for col, val in values.items():
                            updates.append((val, pk, col))
                    else:
                        # Insert new record
                        inserts.append((id_, 237, date, values.get(column_name)))
                
                # Execute batch updates
                for value, pk, col in updates:
                    cursor.execute(f'''UPDATE "dyGEO".{list_}_economic_indicator_log 
                                     SET "{list_}_economic_indicator_{col}" = %s 
                                     WHERE "{list_}_economic_indicator_PK" = %s''', (value, pk))
                
                # Execute batch inserts using COPY for performance
                if inserts:
                    output = io.StringIO()
                    for row in inserts:
                        output.write(f"{row[0]},{row[1]},{row[2]},{row[3] if row[3] is not None else ''}\n")
                    output.seek(0)
                    cursor.copy_expert(f'''COPY "dyGEO".{list_}_economic_indicator_log 
                                         ("{list_}_economic_indicator_ID", "{list_}_economic_indicator_country_PK", 
                                          "{list_}_economic_indicator_date", "{list_}_economic_indicator_{column_name}") 
                                         FROM STDIN WITH (FORMAT CSV)''', output)
                    total_indicators_inserted += len(inserts)
                
                cursor.connection.commit()
    
    # Print summary of processing results
    for interval, indicators in no_values_by_interval.items():
        if indicators:
            print(f"[INFO] Doesn't have values for {interval}: {', '.join(indicators)}")
    
    if total_indicators_inserted > 0:
        print(f"[INFO] Inserted {total_indicators_inserted} new economic indicator records in total")
    
    print("[EXTRACTION] Economic indicators processing completed")
    return indicators_already_available, total_indicators_inserted

# ==============================================================================
# ASSET EVENTS PROCESSING - Earnings, Dividends, Splits, Federal Funds Rate
# ==============================================================================

# Batch insert asset events with deduplication and expected->actual replacement logic
def batch_insert_events(cursor, events, asset_symbol, assets_no_new_events=None):
    """
    Insert asset events (earnings, dividends, splits) with smart deduplication
    Replaces expected earnings with actual results when available.
    If an actual earnings event arrives that corresponds to an existing expected
    placeholder (if dates differ), the expected placeholder will be deleted.
    """
    if not events:
        if assets_no_new_events is not None:
            assets_no_new_events.append(asset_symbol)
        return 0

    # Fetch existing events for this asset
    cursor.execute(f'SELECT "asset_event_PK", "asset_event_event_pk", "asset_event_announcement_date", "asset_event_start_date", "asset_event_event_notes" FROM "ASSET_{asset_symbol}".asset_events_data')
    existing_rows = cursor.fetchall()

    existing_set = set()
    events_to_update = []

    # Helper to extract quarter/year hint from notes or dates
    def _extract_qy(notes_obj, start_date, ann_date):
        q = y = None
        try:
            if isinstance(notes_obj, str):
                try:
                    notes_obj = json.loads(notes_obj)
                except Exception:
                    notes_obj = None
            if isinstance(notes_obj, dict):
                q = notes_obj.get('quarter') or notes_obj.get('q')
                y = notes_obj.get('year') or notes_obj.get('y')
        except Exception:
            pass
        # Derive if missing
        if (q is None or y is None):
            d = start_date or ann_date
            if d:
                q = f"Q{((d.month-1)//3)+1}"
                y = d.year
        return (str(q) if q else None, int(y) if isinstance(y, int) else (int(y) if isinstance(y, str) and y.isdigit() else (y.year if hasattr(y, 'year') else None)))

    # Index expected earnings by (quarter, year) and by start_date for matching
    expected_by_qy = {}
    expected_by_start = {}

    # Build set of existing events with expected/actual flag
    for row in existing_rows:
        pk, event_pk, ann_date, start_date, notes = row
        ann_str = ann_date.strftime('%Y-%m-%d') if ann_date else ''
        start_str = start_date.strftime('%Y-%m-%d') if start_date else ''
        key = (event_pk, ann_str, start_str)

        # Check if this is an expected earnings event
        is_expected = False
        if notes:
            try:
                notes_dict = json.loads(notes) if isinstance(notes, str) else notes
                is_expected = (event_pk == 2) and (('estimate' in notes_dict) or (notes_dict.get('source') == 'earnings_calendar'))
            except Exception:
                is_expected = (event_pk == 2) and (('earnings_calendar' in str(notes)) or ('estimate' in str(notes)))

        existing_set.add((key, pk, is_expected))

        if is_expected:
            q, y = _extract_qy(notes, start_date, ann_date)
            if q and y:
                expected_by_qy.setdefault((q, y), set()).add(pk)
            if start_date:
                expected_by_start.setdefault(start_date, set()).add(pk)

    # Process new events - check for duplicates, expected->actual replacements, and deletions
    new_events = []
    expected_to_delete = set()
    for e in events:
        event_pk = e.get('event_pk')
        ann = e.get('announcement_date')
        start = e.get('start_date')
        ann_str = ann.strftime('%Y-%m-%d') if ann else ''
        start_str = start.strftime('%Y-%m-%d') if start else ''
        key = (event_pk, ann_str, start_str)
        details = e.get('details', {})

        # Check if this event already exists
        existing_match = None
        for existing_key, existing_pk, is_existing_expected in existing_set:
            if existing_key == key:
                existing_match = (existing_pk, is_existing_expected)
                break

        if existing_match:
            existing_pk, is_existing_expected = existing_match
            is_new_actual = (event_pk == 2) and (details is not None) and (details.get('estimate') is None)

            # Replace expected earnings with actual results when dates fully match
            if is_existing_expected and is_new_actual:
                notes_json = json.dumps(sanitize_for_text(details), ensure_ascii=False)
                events_to_update.append((existing_pk, notes_json))
            # Nothing to insert for exact match
        else:
            # If this is an actual earnings event, try to find corresponding expected to delete
            is_new_actual = (event_pk == 2) and (details is not None) and (details.get('estimate') is None)
            if is_new_actual:
                # Try match by quarter/year first, then by start_date
                q = details.get('quarter')
                y = details.get('year')
                if not (q and y):
                    # derive from dates
                    if start:
                        q = f"Q{((start.month-1)//3)+1}"
                        y = start.year
                    elif ann:
                        q = f"Q{((ann.month-1)//3)+1}"
                        y = ann.year
                # Mark any expected placeholders for deletion
                if q and y and (q, y) in expected_by_qy:
                    expected_to_delete.update(expected_by_qy[(q, y)])
                if start and start in expected_by_start:
                    expected_to_delete.update(expected_by_start[start])
            # Completely new event (keep for insertion)
            new_events.append(e)

    # Delete expected placeholders that correspond to newly arriving actual earnings
    if expected_to_delete:
        try:
            cursor.execute(f'DELETE FROM "ASSET_{asset_symbol}".asset_events_data WHERE "asset_event_PK" = ANY(%s)', (list(expected_to_delete),))
        except Exception as _del_err:
            print(f"[WARN] Failed deleting expected placeholders for {asset_symbol}: {_del_err}")

    # Update existing events (expected -> actual) when keys matched exactly
    for pk, notes_json in events_to_update:
        cursor.execute(f'UPDATE "ASSET_{asset_symbol}".asset_events_data SET "asset_event_event_notes" = %s WHERE "asset_event_PK" = %s', (notes_json, pk))

    if not new_events:
        return len(events_to_update)

    # Insert completely new events using bulk COPY
    cursor.execute(f'SELECT COALESCE(MAX("asset_event_PK"), 0) + 1, COALESCE(MAX("asset_event_ID"), 0) + 1 FROM "ASSET_{asset_symbol}".asset_events_data')
    next_pk, next_id = cursor.fetchone()

    # Prepare CSV data for bulk insert
    output = io.StringIO()
    for i, ev in enumerate(new_events):
        details = ev.get('details', {})
        sanitized_details = sanitize_for_text(details)
        if isinstance(sanitized_details, dict) and 'source' in sanitized_details:
            sanitized_details.pop('source', None)
        now = datetime.now().isoformat()
        
        row = [
            str(next_pk + i), str(next_id + i), str(uuid4()), str(ev['event_pk']),
            ev['announcement_date'].strftime('%Y-%m-%d') if ev['announcement_date'] else '',
            ev['start_date'].strftime('%Y-%m-%d') if ev['start_date'] else '',
            '', now, '', now, '1', json.dumps(sanitized_details, ensure_ascii=False)
        ]
        csv.writer(output).writerow(row)

    # Execute bulk insert
    output.seek(0)
    cursor.copy_expert(f'COPY "ASSET_{asset_symbol}".asset_events_data ("asset_event_PK", "asset_event_ID", "asset_event_UUID", "asset_event_event_pk", "asset_event_announcement_date", "asset_event_start_date", "asset_event_creator_PK", "asset_event_creation_date_time", "asset_event_last_modifier_PK", "asset_event_last_modification_date_time", "asset_event_activity_status", "asset_event_event_notes") FROM STDIN WITH (FORMAT CSV)', output)
    
    return len(new_events) + len(events_to_update)

# Main function to process all asset events (earnings, dividends, splits, federal funds)
def process_events(cursor, only_symbols=None):
    """
    Extract, transform and load asset events data
    Handles: Earnings reports, dividends, stock splits, federal funds rate
    Optionally limits processing to only_symbols (list of uppercase tickers)
    """
    print("\n[EXTRACTION] Starting asset events processing...")
    
    # Get all available asset schemas and symbols
    cursor.execute('SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE \'ASSET_%\' AND schema_name != \'ASSET_TEMPLATE\' AND schema_name NOT LIKE \'%=%\' AND schema_name NOT LIKE \'%.%\' AND schema_name NOT LIKE \'%-%\' ORDER BY schema_name')
    schema_symbols = [s[0].replace('ASSET_', '') for s in cursor.fetchall()]
    
    cursor.execute('SELECT COALESCE(apicalls_symbol, financial_asset_alpha_vantage_symbol) AS sym FROM "dyLEARN".financial_asset_list_view WHERE financial_asset_alpha_vantage_symbol IS NOT NULL AND COALESCE(apicalls_symbol, financial_asset_alpha_vantage_symbol) IS NOT NULL ORDER BY sym')
    view_symbols = [r[0].upper() for r in cursor.fetchall()]
    
    # Process only US symbols that have both schema and view entries
    us_symbols = sorted(set(schema_symbols).intersection(view_symbols))

    # Optional filter by provided tickers list
    if only_symbols:
        only_set = set(s.upper() for s in only_symbols)
        us_symbols = [s for s in us_symbols if s in only_set]
        print(f"[INFO] Limiting processing to symbols: {', '.join(us_symbols) if us_symbols else '(none)'}")

    assets_no_new_events, symbols_with_warnings = [], []
    total_events_inserted, events_count, last_reported = 0, 0, 0
    total_federal_funds_inserted = 0

    # Fetch federal funds rate data (applies to all assets)
    federal_funds_rates = []
    try:
        print("[INFO] Fetching federal funds rate data...")
        ff_resp = FEDERAL_FUNDS_RATE('monthly')
        if isinstance(ff_resp, dict):
            parsed = []
            ff_data = ff_resp.get('data')
            
            # Handle list format
            if isinstance(ff_data, list):
                for rec in ff_data:
                    date_str = rec.get('date') 
                    value = rec.get('value') 
                    if date_str and value not in (None, '', '.'):
                        d = parse_date(date_str)
                        if d:
                            parsed.append((d, str(value)))
            else:
                # Handle various nested dictionary formats
                for key in ('data', 'Monthly Time Series', 'Time Series (Monthly)', 'series'):
                    ts = ff_resp.get(key)
                    if isinstance(ts, dict):
                        for date_str, rec in ts.items():
                            if isinstance(rec, dict):
                                val = next((v for v in rec.values() if isinstance(v, (str, int, float)) and str(v) not in ('', '.')), None)
                            else:
                                val = rec
                            
                            if val not in (None, '', '.'):
                                d = parse_date(date_str)
                                if d:
                                    parsed.append((d, str(val)))
                        break
                    elif isinstance(ts, list):
                        for rec in ts:
                            if isinstance(rec, dict):
                                date_str = rec.get('date') 
                                value = rec.get('value')
                                if date_str and value not in (None, '', '.'):
                                    d = parse_date(date_str)
                                    if d:
                                        parsed.append((d, str(value)))
                        break
            
            if parsed:
                # Keep only recent 12 months of data
                parsed = sorted({(d, v) for d, v in parsed}, key=lambda x: x[0], reverse=True)
                federal_funds_rates = parsed[:12]
                print(f"[INFO] Processed {len(federal_funds_rates)} Federal Funds Rate records")
    except Exception as e:
        print(f"[WARN] Exception fetching FEDERAL_FUNDS_RATE: {e}")

    # Process each asset symbol
    for symbol in us_symbols:
        # Verify table access
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "ASSET_{symbol}".asset_events_data')
            cursor.fetchone()[0]
        except Exception as e:
            print(f"[ERROR] Cannot access table for {symbol}: {e}")
            continue
        
        all_events, warnings = [], []

        # Fetch different types of events for this symbol
        for event_type, event_pk, api_func in [
            ('earnings', 2, EARNINGS), ('earnings_calendar', 2, EARNINGS_CALENDAR), 
            ('dividends', 1, DIVIDENDS), ('splits', 3, STOCK_SPLITS)
        ]:
            try:
                resp = api_func(symbol)
                
                # Validate API response
                if not isinstance(resp, dict) or "status" not in resp or resp.get("status") != 200 or "data" not in resp:
                    warnings.append(f"Invalid or failed response for {event_type}")
                    continue
                    
                data = resp["data"]
                
                # Check for API errors
                if isinstance(data, dict) and "error" in data:
                    warnings.append(f"API error for {event_type}: {data['error']}")
                    continue
                
                # Process different event types
                if event_type == 'earnings':
                    # Historical earnings reports
                    if data.get("symbol") and data.get("symbol").upper() != symbol:
                        warnings.append(f"Symbol mismatch in {event_type}: expected {symbol}, got {data.get('symbol')}")
                        continue
                    for e in data.get('quarterlyEarnings', []):
                        fiscal = parse_date(e.get('fiscalDateEnding'))
                        reported = parse_date(e.get('reportedDate'))
                        if fiscal and reported and reported >= fiscal:
                            quarter = get_quarter_from_date(e.get('fiscalDateEnding'))
                            year = fiscal.year if fiscal else None
                            all_events.append({
                                'event_pk': event_pk,
                                'announcement_date': fiscal,
                                'start_date': reported,
                                'details': {'quarter': quarter, 'year': year}
                            })
                            
                elif event_type == 'earnings_calendar':
                    # Future earnings estimates/calendar
                    calendar_data = data
                    
                    if isinstance(calendar_data, str):
                        # Handle CSV string format
                        lines = calendar_data.strip().split('\n')
                        if len(lines) > 1:
                            headers = [h.strip() for h in lines[0].split(',')]
                            symbol_index = next((i for i, h in enumerate(headers) if 'symbol' in h.lower()), None)
                            date_index = next((i for i, h in enumerate(headers) if 'reportdate' in h.lower() or 'date' in h.lower()), None)
                            fiscal_index = next((i for i, h in enumerate(headers) if 'fiscal' in h.lower()), None)
                            estimate_index = next((i for i, h in enumerate(headers) if 'estimate' in h.lower()), None)
                            
                            if symbol_index is not None and date_index is not None:
                                for line in lines[1:]:
                                    cols = [c.strip() for c in line.split(',')]
                                    if len(cols) > max(symbol_index, date_index):
                                        line_symbol = cols[symbol_index]
                                        if line_symbol == symbol:
                                            earnings_date = parse_date(cols[date_index])
                                            fiscal_date = parse_date(cols[fiscal_index]) if fiscal_index and len(cols) > fiscal_index else None
                                            estimate = cols[estimate_index] if estimate_index and len(cols) > estimate_index else None
                                            
                                            if earnings_date:
                                                use_date = fiscal_date or earnings_date
                                                quarter = get_quarter_from_date(use_date.strftime('%Y-%m-%d')) if isinstance(use_date, type(datetime.now().date())) else get_quarter_from_date(cols[fiscal_index] if fiscal_index and len(cols) > fiscal_index else cols[date_index])
                                                year = use_date.year if isinstance(use_date, type(datetime.now().date())) else None
                                                all_events.append({
                                                    'event_pk': event_pk,
                                                    'announcement_date': fiscal_date,
                                                    'start_date': earnings_date,
                                                    'details': {'quarter': quarter, 'year': year, 'estimate': estimate}
                                                })
                                                
                    elif isinstance(calendar_data, list):
                        # Handle list format
                        for e in calendar_data:
                            earnings_date = parse_date(e.get('reportDate') or e.get('date'))
                            earnings_calendar_fiscal = parse_date(e.get('fiscalDateEnding'))
                            if earnings_date:
                                use_date = earnings_calendar_fiscal or earnings_date
                                quarter = get_quarter_from_date(use_date.strftime('%Y-%m-%d')) if use_date else None
                                year = use_date.year if use_date else None
                                all_events.append({
                                    'event_pk': event_pk,
                                    'announcement_date': earnings_calendar_fiscal,
                                    'start_date': earnings_date,
                                    'details': {'quarter': quarter, 'year': year, 'estimate': e.get('estimate')}
                                })
                                
                    elif isinstance(calendar_data, dict):
                        # Handle nested dictionary format
                        inner_data = calendar_data.get('data', [])
                        if isinstance(inner_data, list):
                            for e in inner_data:
                                earnings_date = parse_date(e.get('reportDate') or e.get('date'))
                                earnings_calendar_fiscal = parse_date(e.get('fiscalDateEnding'))
                                if earnings_date:
                                    use_date = earnings_calendar_fiscal or earnings_date
                                    quarter = get_quarter_from_date(use_date.strftime('%Y-%m-%d')) if use_date else None
                                    year = use_date.year if use_date else None
                                    all_events.append({
                                        'event_pk': event_pk,
                                        'announcement_date': earnings_calendar_fiscal,
                                        'start_date': earnings_date,
                                        'details': {'quarter': quarter, 'year': year, 'estimate': e.get('estimate')}
                                    })
                            
                elif event_type == 'dividends':
                    # Dividend payments
                    for d in data.get('data', []):
                        ex_date = parse_date(d.get('ex_dividend_date'))
                        if ex_date:
                            amount = d.get('dividend_amount') or d.get('dividend') or d.get('amount') or d.get('value')
                            all_events.append({
                                'event_pk': event_pk,
                                'announcement_date': ex_date,
                                'start_date': parse_date(d.get('payment_date')) or ex_date,
                                'details': {'dividend_amount': amount}
                            })
                            
                elif event_type == 'splits':
                    # Stock splits
                    for s in data.get('data', []):
                        eff = parse_date(s.get('effective_date'))
                        if eff:
                            factor = (s.get('split_factor') or s.get('split_coefficient') or 
                                    s.get('split_ratio') or s.get('splitFactor') or s.get('factor'))
                            all_events.append({
                                'event_pk': event_pk,
                                'announcement_date': eff,
                                'start_date': eff,
                                'details': {'split_ratio': compute_split_ratio_str(factor)}
                            })
                    
            except Exception as e:
                warnings.append(f"Exception getting {event_type}: {e}")

        # Add federal funds rate events to all assets
        if federal_funds_rates:
            for date, value in federal_funds_rates:
                all_events.append({
                    'event_pk': 4,
                    'announcement_date': date,
                    'start_date': date,
                    'details': {'fvalue': value}
                })

        # Track progress across all events
        events_count += len(all_events)
        if events_count // 1000 > last_reported:
            last_reported = events_count // 1000
            print(f"[INFO] Processed {last_reported * 1000} events so far")

        # Sort and insert events if any exist
        if all_events:
            # Sort events by date and type (actual earnings before expected)
            def sort_key(event):
                date = event.get('announcement_date')
                event_pk = event.get('event_pk', 0)
                date_sort = date.strftime('%Y-%m-%d') if date else '9999-12-31'
                type_priority = event_pk
                if event_pk == 2:
                    details = event.get('details', {})
                    if details and details.get('estimate') is None:
                        type_priority = 2.1  # Actual earnings first
                    else:
                        type_priority = 2.2  # Expected earnings second
                return (date_sort, type_priority)
            
            all_events.sort(key=sort_key)
            
            # Insert events into database
            try:
                inserted = batch_insert_events(cursor, all_events, symbol)
                total_events_inserted += inserted
                ff_events_count = len([e for e in all_events if e.get('event_pk') == 4])
                total_federal_funds_inserted += ff_events_count if inserted > 0 else 0
            except Exception as e:
                warnings.append(f"Failed to insert events: {e}")
        else:
            # No events for this symbol
            batch_insert_events(cursor, [], symbol, assets_no_new_events)

        # Track any warnings for this symbol
        if warnings:
            symbols_with_warnings.append((symbol, warnings))

    # Print final summary
    if total_events_inserted > 0:
        print(f"[INFO] Inserted {total_events_inserted} new events in total for all assets")
    else:
        if events_count == 0:
            print("[INFO] No events processed for any asset")
        else:
            print("[INFO] No new events to insert for any asset - all data already available")
    
    if total_federal_funds_inserted > 0:
        print(f"[INFO] Inserted {total_federal_funds_inserted} federal funds rate events in total for all assets")
    elif federal_funds_rates:
        print("[INFO] Federal funds rate data is already available in database for all assets")
    
    print("[EXTRACTION] Asset events processing completed")
    return total_events_inserted


# ==============================================================================
# MAIN ETL FUNCTION - Entry Point for Economic Indicators Extractor
# ==============================================================================

# Main ETL resolver function - orchestrates the entire data extraction process
def resolve_EIE(_, info, tickers_list=None):
    """
    Main Economic Indicators Extractor function
    
    EXTRACTION: Fetches economic indicators and asset events from various APIs
    TRANSFORMATION: Processes, validates and formats the data  
    LOADING: Inserts/updates data in PostgreSQL database with deduplication
    
    Optional: tickers_list to limit processing to specific symbols
    
    Returns: Success/error status with processing statistics
    """
    try:
        print("\n" + "="*80)
        print("ECONOMIC INDICATORS EXTRACTOR BOT - STARTING ETL PROCESS")
        print("="*80)
        
        # Import settings
        from settings import DBNAME, DATABASE_HOST, USER, PASSWORD, DATABASE_PORT
        
        # Database connection configuration
        connection_params = {
            "host": DATABASE_HOST,
            "port": DATABASE_PORT,
            "user": USER,
            "password": PASSWORD,
            "database": DBNAME
        }

        # Database connection with retry logic
        def connect_with_retries(retries=5, delay=5):
            """Attempt database connection with exponential backoff"""
            for attempt in range(retries):
                try:
                    conn = psycopg2.connect(**connection_params)
                    conn.autocommit = False
                    print(f"[DB] Connected successfully on attempt {attempt + 1}")
                    return conn
                except Exception as e:
                    print(f"[ERROR] DB connection attempt {attempt + 1} failed: {e}")
                    if attempt < retries - 1:
                        time_module.sleep(delay)
            print("[ERROR] All database connection attempts failed")
            return None

        conn = connect_with_retries()
        if conn is None:
            return {'success': False, 'error': 'Database connection failed'}
            
        cursor = conn.cursor()
        print("[DB] Database cursor established")
        
        # Normalize tickers_list from GraphQL (can be list or JSON string)
        only_symbols = None
        if tickers_list:
            if isinstance(tickers_list, str):
                try:
                    parsed = json.loads(tickers_list)
                except Exception:
                    parsed = None
            else:
                parsed = tickers_list
            if isinstance(parsed, list):
                only_symbols = [str(x).upper() for x in parsed if x]
                print(f"[INFO] Received tickers_list: {only_symbols}")
        
        # Configure economic indicators and their supported intervals
        interval_list = ['daily', 'weekly', 'monthly', 'semiannual', 'quarterly', 'annual']
        interval_dict = {
            'real_GDP_interval_list': ['quarterly', 'annual'],
            'real_GDP_pc_interval_list': ['quarterly'],
            'Federal_funds_interval_list': ['daily', 'weekly', 'monthly'],
            'Treasury_yield_interval_list': ['daily', 'weekly', 'monthly'],
            'CPI_interval_list': ['monthly', 'semiannual'],
            'inflation_interval_list': ['annual'],
            'retail_sales_interval_list': ['monthly'],
            'durables_interval_list': ['monthly'],
            'unemployment_interval_list': ['monthly'],
            'nonfarm_payrolls_interval_list': ['monthly']
        }

        # ==============================================================================
        # STEP 1: PROCESS ECONOMIC INDICATORS
        # ==============================================================================
        try:
            indicators_already_available, total_indicators_inserted = process_economic_indicators(
                cursor, interval_list, interval_dict.items(), set(), set())
        except Exception as e:
            print(f"[ERROR] Exception in process_economic_indicators: {e}")
            # Continue with events processing even if indicators fail
            indicators_already_available, total_indicators_inserted = [], 0
        
        # ==============================================================================
        # STEP 2: FORWARD/BACKWARD FILL MISSING VALUES
        # ==============================================================================
        fill_map = {
            'Treasury_yield_interval_list': 'TCMRP_value',
            'Federal_funds_interval_list': 'EFFRP_value',
            'CPI_interval_list': 'CPIAUC_value',
            'inflation_interval_list': 'ICPP_value',
            'retail_sales_interval_list': 'ARSRTMUSD_value',
            'durables_interval_list': 'MNODGMUSD_value',
            'unemployment_interval_list': 'URP_value',
            'nonfarm_payrolls_interval_list': 'TNPTP_value',
            'real_GDP_interval_list': 'RGDPBUSD_value',
            'real_GDP_pc_interval_list': 'RGDPCBUSD_value'
        }
        prefix_map = {'daily': 'D', 'weekly': 'W', 'monthly': 'M', 'quarterly': 'Q', 'semiannual': 'SA', 'annual': 'A'}
        
        print("\n[TRANSFORMATION] Applying forward/backward fill for missing values...")
        for interval in interval_list:
            for indicator, intervals in interval_dict.items():
                if interval in intervals and indicator in fill_map:
                    column_name = prefix_map[interval] + fill_map[indicator]
                    table_name = f'"dyGEO".{interval}_economic_indicator_log'
                    try:
                        forward_backward_fill_indicator(cursor, table_name, column_name, interval)
                        conn.commit()
                    except Exception as e:
                        print(f"[WARN] Forward/backward fill failed for {column_name}: {e}")
                        try:
                            conn.rollback()
                        except Exception:
                            pass

        # Print economic indicators summary
        if total_indicators_inserted > 0:
            print(f"\n[LOAD] Economic indicators summary: {total_indicators_inserted} new records inserted")
        elif indicators_already_available:
            print(f"\n[LOAD] Economic indicators summary: Data already available for recent dates")
        else:
            print("\n[LOAD] Economic indicators summary: No new records inserted")

        # ==============================================================================
        # STEP 3: PROCESS ASSET EVENTS
        # ==============================================================================
        total_events_inserted = process_events(cursor, only_symbols)

        # ==============================================================================
        # STEP 4: FINALIZE AND CLEANUP
        # ==============================================================================
        print("\n[LOAD] Committing all transactions...")
        try:
            conn.commit()
            print("[LOAD] All transactions committed successfully")
        except Exception as e:
            print(f"[ERROR] Failed to commit transactions: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        
        # Close database connections
        try:
            cursor.close()
            conn.close()
            print("[DB] Database connections closed")
        except Exception as e:
            print(f"[WARN] Error closing database connections: {e}")
        
        # Final results
        result = {
            "success": True,
            "indicators_inserted": int(total_indicators_inserted),
            "events_inserted": int(total_events_inserted),
            "message": "ETL process completed successfully"
        }
        
        print("\n" + "="*80)
        print("ETL PROCESS COMPLETED SUCCESSFULLY")
        print(f"Economic indicators inserted: {total_indicators_inserted}")
        print(f"Asset events inserted: {total_events_inserted}")
        print("="*80)
        
        return result
        
    except Exception as e:
        # Handle any unexpected errors
        print(f"\n[ERROR] Critical error in resolve_EIE: {e}")
        try:
            return {
                "success": False, 
                "error": str(e),
                "message": "ETL process failed due to unexpected error"
            }
        except Exception:
            return {
                "success": False, 
                "error": "Unknown error occurred",
                "message": "ETL process failed"
            }


# ==============================================================================
# SCRIPT ENTRY POINT - For standalone execution
# ==============================================================================
# if __name__ == "__main__":
#     print("Starting Economic Indicators Extractor Bot...")
#     response = resolve_EIE(None, None)
#     print("\nFinal Response:", response)