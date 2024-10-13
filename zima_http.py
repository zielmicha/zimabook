from zima_core import Notebook, VarMeta, CellState
import pathlib, flask, lxml.html, lxml.builder, lxml.etree, urllib.parse
import serve_table, zima_core
import os
import subprocess
from flask_socketio import SocketIO
import threading
import time

class _E:
    def __getattr__(self, elem):
        def elem_builder(*children, **kwargs):
            real_children = []
            
            for ch in children:
                if isinstance(ch, dict):
                    kwargs.update(ch)
                elif isinstance(ch, list):
                    real_children += ch
                else:
                    real_children.append(ch)

            kwargs = {
                k.replace('_', '-').rstrip('_'):v
                for k, v in kwargs.items()
            }
            return getattr(lxml.builder.E, elem)(*real_children, **kwargs)

        return elem_builder

E = _E()

def read_log_file(log_path, max_lines=1000, max_line_length=1000):
    if not os.path.exists(log_path):
        return None
    
    try:
        result = subprocess.run(['tail', '-n', str(max_lines), log_path], capture_output=True, text=True)
        log_content = result.stdout
        
        truncated_lines = [line[:max_line_length] for line in log_content.splitlines()]
        return '\n'.join(truncated_lines)
    except Exception as e:
        return f"Error reading log file: {str(e)}"

def run_http_server(notebook, port):
    app = flask.Flask('zima')
    socketio = SocketIO(app)

    def get_table_filename(args):
        hash = args['hash']
        return notebook.var_storage.get_var_parquet(hash)
        
    serve_table.install(app, get_table_filename)
    
    def render_var(var):
        meta: VarMeta = notebook.var_storage.get_var_meta(var[2])

        if meta['kind'] == 'parquet':
            hash = notebook.get_var_hash(var[1])
            return getattr(E, 'data-table')(server_url='/data?hash=' + urllib.parse.quote(hash))
            
        return E.div(
            f"Name: {var[1]}, Value: {notebook.var_storage.get_var_repr(var[2])}",
            class_="variable"
        )

    def render_log(title, path):
        data = read_log_file(path)
        if data is not None:
            return E.div(title, E.pre(data))
        return E.div()
    
    def render_cell(cell_id, cell, variables):
        cell_state = notebook.get_cell_state(cell_id)

        freshness = [
            ('preamble', cell_state.preamble_fresh),
            ('code', cell_state.code_fresh),
            ('deps', cell_state.dep_fresh),
        ]
        stale = [ name for name, is_ in freshness if not is_ ]
        freshness_html = E.div('stale', title=', '.join(stale)) if stale else 'fresh'

        return E.div(
            E.div(E.span(f"Cell ID: {cell_id}", class_="cell-id")),
            E.pre(cell.code),
            E.div(
                E.div("Variables:"),
                [render_var(var)
                 for var in variables if var[0] == cell_id],
                class_="variables"
            ),
            E.div(
                E.div("Cell State:"),
                freshness_html,
                render_log('Current log', cell_state.current_log),
                render_log('Pending log', cell_state.pending_log),
                class_="cell-state"
            ),
            E.button(
                "Run Cell",
                class_="run-cell-button",
                onclick=f"runCell('{cell_id}')"
            ),
            E.hr(),
            class_="cell"
        )
    
    @app.route('/')
    def index():
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Zima Notebook</title>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
            <script src="https://unpkg.com/morphdom@2.6.1/dist/morphdom-umd.min.js"></script>        
            <script src="/deps/jquery-3.6.0.min.js"></script>
            <script src="/deps/jquery.dataTables.min.js"></script>
            <script src="/static/serve_table.js"></script>
            <style>
                body { font-family: Arial, sans-serif; }
                .cell { border: 1px solid #ddd; margin: 10px 0; padding: 10px; }
                .cell-id { font-weight: bold; }
                pre { background-color: #f0f0f0; padding: 10px; }
                .variables { margin-top: 20px; }
                .variable { margin-bottom: 5px; }
                .cell-state { margin-top: 10px; font-style: italic; }
            </style>
            <script>
                const socket = io();
                socket.on('update', function(data) {
                    const contentElement = document.getElementById('content');
                    morphdom(contentElement, data, {childrenOnly: true});
                });

                function runCell(cellId) {
                    socket.emit('run_cell', {cell_id: cellId});
                }
            </script>
        </head>
        <body>
            <div id="content">Loading...</div>
        </body>
        </html>
        """

    @app.route('/deps/<path:filename>')
    def serve_deps(filename):
        return flask.send_from_directory('deps', filename)
    
    @app.route('/static/<path:filename>')
    def serve_static(filename):
        return flask.send_from_directory('static', filename)
    
    def render_notebook():
        with notebook._db() as conn:
            variables = conn.execute('SELECT owner_cell, name, data_hash FROM vars').fetchall()
    
        var_storage = zima_core.VarStorage(notebook._data_dir)

        html = E.div(    E.h1("Zima Notebook"),
                [
                    render_cell(cell_id, cell, variables)
                    for cell_id, cell in notebook.notebook_def.cells.items()
                ]
            )
        
        return lxml.html.tostring(html, pretty_print=True).decode()

    def send_updates():
        while True:
            html_content = render_notebook()
            socketio.emit('update', html_content)
            time.sleep(1)

    @socketio.on('run_cell')
    def handle_run_cell(data):
        cell_id = data['cell_id']
        notebook.execute_cell(cell_id)

    update_thread = threading.Thread(target=send_updates)
    update_thread.daemon = True
    update_thread.start()

    app.run(port=port, debug=True, use_reloader=False)
