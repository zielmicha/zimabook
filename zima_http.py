from zima_core import Notebook, VarMeta, CellState
import pathlib, flask, lxml.html, lxml.builder, lxml.etree, urllib.parse
import serve_table, zima_core, token_auth
import os
import subprocess
from flask_socketio import SocketIO
import threading
import time
import json
from typing import Literal

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
                if v is not None
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
    token_auth.install(app, socketio, app_name='zima')

    def get_table_filename(args):
        hash = args['hash']
        return notebook.var_storage.get_var_parquet(hash)
        
    serve_table.install(app, get_table_filename, decorator=token_auth.token_required)
    
    # session vars
    update_event = threading.Condition()
    mode: Literal['command'] | Literal['edit']  = 'command'
    current_cell_id = None
    epoch = 0
    
    def _update():
        nonlocal epoch
        with update_event:
            epoch += 1
            update_event.notify_all()
            
    def render_var(var):
        meta: VarMeta = notebook.var_storage.get_var_meta(var[2])

        if meta['kind'] == 'parquet':
            hash = notebook.get_var_hash(var[1])
            content = getattr(E, 'data-table')(server_url='/data?hash=' + urllib.parse.quote(hash))
        else:
            content = notebook.var_storage.get_var_repr(var[2])
            
        return E.div(
            f"{var[1]} = ",
            content,
            class_="variable"
        )

    def render_log(title: Literal['pending'] | Literal['current'], path):
        data = read_log_file(path)
        if data is None:
            return E.div()
        non_empty = data.strip()
        log_content = E.pre(data) if non_empty else E.div()
        
        if title == 'pending':
            return E.div('Pending', log_content)
        else:
            return log_content
    
    def render_cell(cell_id, cell, variables):
        cell_state = notebook.get_cell_state(cell_id)

        freshness = [
            ('preamble', cell_state.preamble_fresh),
            ('code', cell_state.code_fresh),
            ('deps', cell_state.dep_fresh),
        ]
        stale = [ name for name, is_ in freshness if not is_ ]
        freshness_html = E.div('stale', title=', '.join(stale)) if stale else 'fresh'

        is_focused = cell_id == current_cell_id 

        return E.div(
            E.div(E.span(f"Cell ID: {cell_id}", class_="cell-id")),
            getattr(E, 'textarea-wrapper')(
                text=cell.code,
                id=f"code_{cell_id}",
                modify_event=json.dumps({'name': 'save_code', 'params': {'cell_id': cell_id}}),
                focus_event=json.dumps({'name': 'focus_cell', 'params': {'cell_id': cell_id}}),
                focus='focus' if is_focused and mode == 'edit' else None),
            E.div(
                [render_var(var)
                 for var in variables if var[0] == cell_id],
                class_="variables"
            ),
            E.div(
                E.div("Cell State:"),
                freshness_html,
                render_log('current', cell_state.current_log),
                render_log('pending', cell_state.pending_log),
                class_="cell-state"
            ),
            E.button(
                "Run Cell",
                class_="run-cell-button",
                onclick=f"runCell('{cell_id}')"
            ),
            E.hr(),
            id='cell_' + cell_id,
            **{'class': 'cell focused scroll-to' if is_focused else 'cell'},
        )
    
    @app.route('/')
    @token_auth.token_required
    def index():
        return flask.send_from_directory('static', 'index.html')

    @app.route('/deps/<path:filename>')
    def serve_deps(filename):
        return flask.send_from_directory('deps', filename)
    
    @app.route('/static/<path:filename>')
    def serve_static(filename):
        return flask.send_from_directory('static', filename)
    
    def render_notebook():
        nonlocal current_cell_id
        with notebook._db() as conn:
            variables = conn.execute('SELECT owner_cell, name, data_hash FROM vars').fetchall()

        if current_cell_id is None:
            cell_ids = list(notebook.notebook_def.cells.keys())
            if cell_ids:
                current_cell_id = cell_ids[0] 
            
        var_storage = zima_core.VarStorage(notebook._data_dir)

        html = E.div(
            E.h1("Zima Notebook"),
            [
                render_cell(cell_id, cell, variables)
                for cell_id, cell in notebook.notebook_def.cells.items()
            ]
        )
        
        return lxml.html.tostring(html, pretty_print=True).decode()
    
    def send_updates():
        while True:
            this_epoch = epoch
            html_content = render_notebook()
            socketio.emit('update', html_content)
            with update_event:
                update_event.wait_for(lambda: epoch != this_epoch, timeout=1)

    @socketio.on('run_cell')
    def handle_run_cell(data):
        cell_id = data['cell_id']
        notebook.execute_cell(cell_id)

    @socketio.on('save_code')
    def handle_save_code(data):
        cell_id = data['cell_id']
        new_code = data['content']
        notebook.modify_cell_code(cell_id, new_code)
        socketio.emit('code_saved', {'cell_id': cell_id})

        _update()

    @socketio.on('focus_cell')
    def handle_focus_cell(data):
        nonlocal current_cell_id, mode
        current_cell_id = data['cell_id']
        mode = 'edit'
        
        _update()
        
    @socketio.on('loaded')
    def handle_loaded(data):
        print('loaded')
        _update()
                
    @socketio.on('keydown')
    def handle_keydown(data):
        nonlocal current_cell_id, mode
        key = data['key']

        if key in ('ArrowDown', 'ArrowUp') and mode == 'command':
            cell_ids = list(notebook.notebook_def.cells.keys())
            try:
                index = cell_ids.index(current_cell_id)
            except ValueError:
                pass
            else:
                index += {'ArrowDown': 1, 'ArrowUp': -1}[key]
                if index >= 0 and index < len(cell_ids):
                    current_cell_id = cell_ids[index]

        
        if key == 'Ctrl+Enter':
            notebook.execute_cell(current_cell_id)
                    
        if key == 'Enter' and mode == 'command':
            mode = 'edit'

        if key == 'Escape' and mode == 'edit':
            mode = 'command'
                    
        _update()
                
    update_thread = threading.Thread(target=send_updates)
    update_thread.daemon = True
    update_thread.start()

    app.run(port=port, debug=True, use_reloader=False)
