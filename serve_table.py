from flask import Flask, render_template, request, jsonify, send_from_directory
import polars as pl, pathlib

def install(app, get_filename, decorator=lambda f: f):
    @app.route('/data', methods=['POST'])
    @decorator
    def data():
        parquet_file_path = get_filename(request.args)
        # Get DataTables parameters from the request
    
        if request.values.get('get-columns'):
            df_sample = pl.read_parquet(parquet_file_path, n_rows=1)
            columns = df_sample.columns
            return jsonify(columns)
        
        draw = int(request.values.get('draw', 1))
        start = int(request.values.get('start', 0))
        length = int(request.values.get('length', 10))
        search_value = request.values.get('search[value]', '')
    
        # Get ordering parameters
        order_column_index_str = request.values.get('order[0][column]')
        order_direction = request.values.get('order[0][dir]', 'asc')
    
        # Fetch columns dynamically
        df_sample = pl.read_parquet(parquet_file_path, n_rows=1)
        columns = df_sample.columns
    
        order_column = None
        if order_column_index_str:
            order_column_index = int(order_column_index_str)
            if 0 <= order_column_index < len(columns):
                order_column = columns[order_column_index]
    
        # Build the lazy DataFrame
        df_lazy = pl.scan_parquet(parquet_file_path)
    
        # Total number of records before filtering
        total_records = df_lazy.select(pl.count()).collect().item()
    
        
        # Apply global search filter if provided
        if search_value:
            # Build a combined filter expression for all columns
            filter_expr = pl.lit(False)
            for col in columns:
                expr = pl.col(col).cast(pl.Utf8).str.contains(search_value, literal=True, strict=False)
                filter_expr = filter_expr | expr
            df_lazy = df_lazy.filter(filter_expr)
    
        # Total number of records after filtering
        records_filtered = df_lazy.select(pl.count()).collect().item()
    
        # Apply ordering
        if order_column:
            df_lazy = df_lazy.sort(order_column)
            if (order_direction == 'desc'):
                df_lazy = df_lazy.reverse()
    
        # Apply pagination
        df_page = df_lazy.slice(start, length).collect()
    
        # Convert DataFrame to list of dicts
        data = df_page.to_dicts()
    
        # Prepare the response in DataTables format
        response = {
            'draw': draw,
            'recordsTotal': total_records,
            'recordsFiltered': records_filtered,
            'data': data,
        }
    
        return jsonify(response)

if __name__ == '__main__':
    app = Flask(__name__)

    @app.route('/deps/<path:filename>')
    def serve_deps(filename):
        return send_from_directory('deps', filename)
    
    @app.route('/static/<path:filename>')
    def serve_static(filename):
        return send_from_directory('static', filename)
    
    # Load the Parquet file lazily
    parquet_file_path = '/home/michal/tmp/files.parquet'
    
    @app.route('/')
    def index():
        return (pathlib.Path(__file__).parent / 'serve_table.html').read_text()

    install(app, lambda _args: parquet_file_path)

    app.run(debug=True)
