from zima_core import Notebook
import pathlib, argparse
import zima_core

def load_and_run_server(ns):
    import zima_http
    notebook = Notebook(pathlib.Path(ns.notebook_file))
    zima_http.run_http_server(notebook, ns.port)

def debug_run_cell(notebook_file, cell_id):
    notebook = Notebook(pathlib.Path(notebook_file))
    thread = notebook.execute_cell(cell_id)
    thread.join()
    
def main():
    parser = argparse.ArgumentParser(description="")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    parser_1 = subparsers.add_parser("internal-execute", help="Internal")
    parser_1.add_argument("payload_file")
    parser_1.add_argument("output_file")
    
    parser_2 = subparsers.add_parser("run-server", help="Run notebook server")
    parser_2.add_argument("notebook_file")
    parser_2.add_argument("--port", type=int, default=7400)

    parser_3 = subparsers.add_parser("debug-run-cell", help="Run a single cell of a notebook")
    parser_3.add_argument("notebook_file")
    parser_3.add_argument("cell_id")
    
    ns = parser.parse_args()

    if ns.command == "run-server":
        load_and_run_server(ns)        
    elif ns.command == "internal-execute":
        zima_core.internal_execute(ns.payload_file, ns.output_file)
    elif ns.command == "debug-run-cell":
        debug_run_cell(ns.notebook_file, ns.cell_id)
    else:
        parser.print_help()
    
if __name__ == '__main__':
    main()
