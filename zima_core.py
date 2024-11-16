from dataclasses import dataclass, replace
from typing import Any, Optional, Protocol, TypedDict, Literal
import hashlib, os, sys, itertools, functools, cloudpickle, contextlib, pickle, pathlib, shortuuid, shutil, json, base64, polars as pl, pandas as pd, re, tempfile, pyarrow as pa, pyarrow.parquet as pq, types, subprocess, threading, ast, sqlite3, argparse, duckdb, fcntl, logging, errno, string


SCHEMA_SQL = '''

create table if not exists cell_state (
    id text primary key,

    last_refresh_failed bool,
    last_refresh timestamp,

    preamble_hash text,
    code_hash text,
    dep_fresh bool default false
);

create table if not exists deps (
    cell_id text,
    dep_var_name text
);
create index if not exists idx_deps_var_name on deps (dep_var_name);

create table if not exists vars (
    owner_cell text,
    name text,
    data_hash text
);

create index if not exists idx_vars_owner_cell on vars (owner_cell, name);
create unique index if not exists idx_vars_name on vars (name);

'''

def log(text, *args):
    print(text % args)

class Dialect(Protocol):
    def execute(self,
                preamble_module: Any,
                code: str,
                var_hashes: dict[str, str],
                var_storage: 'VarStorage'): ...

class PythonDialect(Dialect):
    def execute(self,
                preamble_module: Any,
                code: str,
                var_hashes: dict[str, str],
                var_storage: 'VarStorage'):
        locals: dict[str, Any] = {}

        def get_var(name):
            try:
                return var_storage.load_as_python(var_hashes[name])
            except Exception as exc:
                # so Python doesn't convert our KeyError to NameError
                raise Exception('while deserializing variable %r' % name) from exc

        locals: dict = {}
        loaded_vars: dict = {}
            
        class VarDict(dict):
            def __setitem__(self, name, value):
                locals[name] = value
                
            def __getitem__(self, name):
                if name in locals:
                    return locals[name]
                elif name in preamble_module.__dict__:
                    return preamble_module.__dict__[name]
                elif name in loaded_vars:
                    return loaded_vars[name]
                else:
                    if name in var_hashes:
                        v = get_var(name)
                        loaded_vars[name] = v    
                        return v
                    raise KeyError(name)

        exec(code, VarDict()) # type: ignore
        
        if '__all__' in locals:
            created_var_names = locals['__all__'] 
        else:
            created_var_names = [ k for k in locals.keys() if not k.startswith('_') ]

        created_vars = {}
        for name in created_var_names:
            try:
                created_vars[name] = var_storage.write_python(locals[name])
            except Exception as exc:
                raise Exception('failed to serialize variable %r' % name) from exc
                
        return ExecutorOutput(
            accessed_vars=set(loaded_vars.keys()),
            created_vars=created_vars,
        )

@dataclass
class NotebookDef:
    data_path: str
    preamble_python: str
    preamble_module: Any
    cells: dict[str, 'CellDef']

@dataclass
class ExecutorPayload:
    preamble_module: str
    code: str
    dialect: Dialect
    vars: dict[str, str] # name to hash
    data_dir: pathlib.Path
    
@dataclass
class ExecutionState:
    preamble_hash: str
    vars: dict[str, str]
    code_hash: str

    process: subprocess.Popen

@dataclass
class ExecutorOutput:
    # on error, executor is supposed to just exit with non-zero status
    accessed_vars : set[str]
    created_vars : dict[str, str] # name to hash 

@dataclass
class CellState:
    current_log: pathlib.Path
    pending_log: pathlib.Path
    preamble_fresh: bool
    code_fresh: bool
    dep_fresh: bool
    var_hashes: dict[str, str]

def parse_cell_header(code: str) -> dict[str, str]:
    tree = ast.parse(code)
    
    result: dict[str, str] = {}
    
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    result[target.id] = ast.unparse(node.value)
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Name):
            result[node.value.id] = 'True'
        else:
            raise Exception('invalid header fragment (%r)' % ast.unparse(node))
            
    return result

@dataclass
class CellDef:
    id: str
    dialect: Dialect
    code: str
    code_hash: str
    args_code: dict[str, str]
    dep_refresh: bool = False
    refresh_every: Optional[float] = None

def unparse_args(args_code):
    result = []
    for k, v in args_code.items():
        if v == 'True':
            result.append(k)
            continue
        
        result.append('%s=%s' % (k, v))

    return '; '.join(result)
    
def unparse_cell(cell: CellDef) -> str:
    assert '\n#%cell ' not in cell.code

    header = '#%%cell %s %s' % (cell.id, unparse_args(cell.args_code))
    
    return header + '\n' + cell.code + '\n'
    
def parse_cell(preamble_module, cell_text):
    cell_header, code = cell_text.split('\n', 1)
    splt = cell_header.split(None, 1)
    cell_id, cell_header = (splt + ['']) if len(splt) == 1 else splt

    if not re.match("^[a-zA-Z0-9]+$", cell_id):
        raise ValueError("Invalid cell_id: Only alphanumeric characters are allowed. (%r)" % cell_id)
    
    code = code.strip()
    raw_header = parse_cell_header(cell_header)
    header = { k:eval(v, preamble_module.__dict__) for k,v in raw_header.items() }

    unknown_keys = header.keys() - {'dep_refresh', 'refresh_every', 'dialect'}
    if unknown_keys:
        raise Exception('unknown attributes specified (%r in %r)' % (unknown_keys, cell_header))

    code_hash = hash_string(raw_header.get('dialect', '') + '\n' + code)
    
    return CellDef(id=cell_id, code=code, code_hash=code_hash,
                   args_code=raw_header, **header)    
    
def parse_notebook(s, data_path) -> NotebookDef:
    parts = s.split('\n#%cell ')
    preamble_code = parts[0]
    cell_texts = parts[1:]

    preamble_module: Any = types.ModuleType('notebook')
    exec(preamble_code, preamble_module.__dict__)

    preamble_module.PythonDialect = PythonDialect
    
    cells = {}
    
    for cell_text in cell_texts:
        cell = parse_cell(preamble_module, cell_text)
        if cell.id in cells:
            raise Exception('duplicate cell %r' % cell.id)
        cells[cell.id] = cell

    return NotebookDef(
        data_path=data_path,
        preamble_python = preamble_code,
        preamble_module = preamble_module,
        cells = cells
    )
        

def hash_string(blob):
    if isinstance(blob, str):
        blob = blob.encode('utf8')
    return hashlib.sha256(blob).hexdigest()

@contextlib.contextmanager
def atomic_open_for_writing(path, mode='w'):
    directory, filename = os.path.split(path)
    with tempfile.NamedTemporaryFile(mode=mode, dir=directory, delete=False) as temp_file:
        try:
            yield temp_file
            temp_file.close()
            os.rename(temp_file.name, path)
        except:
            try:
                os.unlink(temp_file.name)
            except FileNotFoundError: pass

            raise
        
def hash_file_or_dir(path):
    if os.path.islink(path):
        raise OSError('unexpected symlink')
    elif os.path.isdir(path):
        return hash_directory(path)
    else:
        return hash_file(path)

def hash_file(filename):
    hasher = hashlib.sha256()
    hasher.update(b'FILE\n')
    chunk_size = 1024*1024
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def hash_directory(directory : pathlib.Path):
    hasher = hashlib.sha256()
    hasher.update(b'DIR\n')

    children = os.listdir(directory)
    for name in sorted(children):
        hasher.update(base64.b64encode(name.encode('utf8')))
        hasher.update(b'\n')
        hasher.update(hash_file_or_dir(directory / name).encode())
        hasher.update(b'\n')

    return hasher.hexdigest()

VarKindLiteral = Literal['pickle'] | Literal['parquet']

class VarMeta(TypedDict):
    kind: VarKindLiteral

def setup_duckdb(tempdir):
    d = duckdb.connect()
    d.execute("SET temp_directory = '?';", (tempdir, )).fetchone()
    d.execute("SET preserve_insertion_order = false;").fetchone()
    return d

duckdb_invalid_table_regex = re.compile('^Catalog Error: Table with name ([^ ]+) does not exist$')

def execute_query_with_dynamic_tables(d, query, preamble_module, setup_table):
    # it could be possible to do it via overriding globals with a custom object, but
    # DuckDB calls PyDict_Contains directly, so instead we try to execute it and
    # parse "table not found error" multiple times
    while True:
        try:
            return eval('d.execute(query)', preamble_module.__dict__, {'query': query})
        except duckdb.CatalogException as err:
            m = re.match(duckdb_invalid_table_regex, str(err))
            if m:
                name = m.group(1)
                setup_table(d, name)
                continue
            raise
        
class VarStorage:
    def __init__(self, dir : pathlib.Path):
        self.dir = dir
        self.temp_dir = dir / "temp"
        
    def hash_path(self, hash):
        assert type(hash) == str, hash
        return self.dir / ("h_" + hash) 

    def hash_meta(self, hash):
        path = self.hash_path(hash)
        with open(path / 'meta.json') as f: 
            var_meta : VarMeta = json.load(f)

        return path, var_meta
    
    def load_as_python(self, hash):
        path, var_meta = self.hash_meta(hash)
        if var_meta['kind'] == 'pickle':
            with open(path / 'data.pickle', 'rb') as f:
                return pickle.load(f)
        elif var_meta['kind'] == 'parquet':
            return pl.scan_parquet(path / 'data.parquet', glob=False)
        else:
            raise Exception('unknown var kind (%r)' % var_meta.kind)

    def get_var_meta(self, hash) -> VarMeta:
        assert len(hash) == 64 and all( l in string.hexdigits for l in hash )
        _path, var_meta = self.hash_meta(hash)
        return var_meta
        
    def get_var_repr(self, hash):
        path, var_meta = self.hash_meta(hash)
        if var_meta['kind'] == 'pickle':
            return (path / "repr.txt").read_text()
        elif var_meta['kind'] == 'parquet':
            return f"Parquet file: {path / 'data.parquet'}"
        else:
            return f"Unknown variable type: {var_meta['kind']}"
    
    def get_var_parquet(self, hash):
        path, var_meta = self.hash_meta(hash)
        if var_meta['kind'] == 'parquet':
            return path / "data.parquet"
        else:
            raise Exception("not parquet variable")
        
    def write_python(self, data):
        def writer_to_parquet():
            if isinstance(data, pa.Table):
                return lambda output_path: pq.write_table(data, output_path)
            elif isinstance(data, pd.DataFrame):
                return lambda output_path: data.to_parquet(output_path)
            elif isinstance(data, pl.DataFrame):
                return lambda output_path: data.write_parquet(output_path)
            elif  isinstance(data, pl.LazyFrame):
                return lambda output_path: data.sink_parquet(output_path)


        to_parquet = writer_to_parquet()
        result_hash = [None]
        with self.with_dir(result_hash) as dir:
            if to_parquet:
                output_path = dir / "data.parquet"
                to_parquet(output_path)
                kind = 'parquet'
            else:
                with open(dir / "data.pickle", 'wb') as f:
                    pickle.dump(data, f)

                (dir / "repr.txt").write_text(repr(data))
                kind = 'pickle'

            (dir / "meta.json").write_text(json.dumps({'kind': kind}))

        return result_hash[0]
                
    @contextlib.contextmanager
    def with_dir(self, put_hash_here):
        loc = self.temp_dir / (shortuuid.uuid())
        os.mkdir(loc)
        try:
            yield loc
        except Exception:
            shutil.rmtree(loc)
            raise

        hash = hash_file_or_dir(loc)
        put_hash_here[0] = hash
        path = self.hash_path(hash)
        try:
            os.rename(loc, path)
        except FileExistsError:
            shutil.rmtree(loc)
        except OSError as err:
            if err.errno == errno.ENOTEMPTY:
                # target exists
                shutil.rmtree(loc)
            else:
                raise
            
    def remove_remaining(self, active_hashes):
        raise NotImplemented

def _synchronized(f):
    # we love java
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return f(self, *args, **kwargs)
    return wrapper
        
class Notebook:
    def __init__(self, notebook_path : pathlib.Path):
        data_dir = pathlib.Path(str(notebook_path) + ".data").absolute()
        self._notebook_path = notebook_path
        self._data_dir = data_dir
        self._logs_dir = data_dir / "logs"
        self._lock_datadir()
        self._logs_dir.mkdir(exist_ok=True)

        self._temp_dir = data_dir / "temp"
        self._temp_dir.mkdir(exist_ok=True)

        self._pending_execution: dict[str, ExecutionState] = {}
        self.var_storage = VarStorage(self._data_dir)
        
        with self._db() as conn:
            conn.executescript(SCHEMA_SQL)

        self._lock = threading.RLock()
        self.reload_notebook()

    @_synchronized
    def _update_notebook(self, cell_id, new_def: NotebookDef):
        cells_text = [self.notebook_def.preamble_python]
        for cell in new_def.cells.values():
            cells_text.append(unparse_cell(cell))
        
        notebook_text = '\n'.join(cells_text)
        
        with atomic_open_for_writing(self._notebook_path) as f:
            f.write(notebook_text)
        
        self.reload_notebook()

    @_synchronized
    def modify_cell_code(self, cell_id, code):
        if cell_id not in self.notebook_def.cells:
            raise ValueError(f"Cell with id {cell_id} not found")
        
        new_cell = replace(self.notebook_def.cells[cell_id],
                           code=code)
        
        new_def = replace(self.notebook_def,
            cells={**self.notebook_def.cells, cell_id: new_cell}
        )
        
        self._update_notebook(cell_id, new_def)
        
    @_synchronized
    def get_cell_state(self, cell_id) -> CellState:
        with self._db() as conn:
            row = conn.execute('''
                SELECT preamble_hash, code_hash,dep_fresh 
                FROM cell_state
                WHERE id = ?
            ''', (cell_id,)).fetchone()
        
        if row is None:
            raise ValueError(f"Cell with id {cell_id} not found")
        
        preamble_hash, code_hash, dep_fresh = row
        
        current_log = self._logs_dir / f"{cell_id}.current.log"
        pending_log = self._logs_dir / f"{cell_id}.pending.log"
        
        cell_def = self.notebook_def.cells[cell_id]

        with self._db() as conn:
            var_hashes = dict(conn.execute('''
                SELECT name, data_hash
                FROM vars
                WHERE owner_cell = ?
            ''', (cell_id,)).fetchall())

        return CellState(
            current_log=current_log,
            pending_log=pending_log,
            preamble_fresh=preamble_hash == self._preamble_hash,
            code_fresh=code_hash == cell_def.code_hash,
            dep_fresh=dep_fresh,
            var_hashes=var_hashes
        )

    def get_var_hash(self, name):
        with self._db() as conn:
            row = conn.execute('''
                SELECT data_hash 
                FROM vars
                WHERE name = ?
            ''', (name,)).fetchone()
            if not row:
                raise KeyError(name)
            data_hash, = row
            return data_hash

    
    def reload_notebook(self):
        content = self._notebook_path.read_text()
        self.notebook_def = parse_notebook(content, self._data_dir)
        self._preamble_hash = hash_string(self.notebook_def.preamble_python)
        self._reloaded_notebook_def()

    def _lock_datadir(self):
        self._lock_fd = open(self._data_dir / "lock", 'w')
        fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        
    @contextlib.contextmanager
    def _db(self):        
        db = sqlite3.connect(self._data_dir / 'state.sqlite3')
        db.execute('pragma journal_mode = WAL;');
        db.cursor().execute('begin immediate;')
        yield db.cursor()
        db.commit()

    @_synchronized
    def _reloaded_notebook_def(self):
        with self._db() as conn:                 
            existing_cell_ids = [ id for id, in conn.execute('select id from cell_state').fetchall() ]
    
            for cell_id in existing_cell_ids - self.notebook_def.cells.keys():
                self._drop_cell(conn, cell_id)
    
            for cell_id in self.notebook_def.cells.keys() - existing_cell_ids:
                # most fields null, as we haven't yet executed it
                conn.execute('insert into cell_state (id) values (?)', (cell_id,))

    @_synchronized
    def execute_cell(self, cell_id):
        if cell_id in self._pending_execution:
            raise Exception('cell already running %r' % cell_id)
        
        temp_base_path = self._temp_dir / shortuuid.uuid()
        in_path = str(temp_base_path) + '.in'
        out_path = str(temp_base_path) + '.out'

        cmd = [sys.executable, sys.argv[0],
               'internal-execute', '--', in_path, out_path]

        log_file = self._logs_dir / (cell_id + '.pending.log')
        cell_def = self.notebook_def.cells[cell_id]

        with self._db() as conn:
            vars = dict(conn.execute('select name, data_hash from vars').fetchall())

        executor_payload = ExecutorPayload(
            preamble_module=self.notebook_def.preamble_module,
            code=cell_def.code,
            dialect=cell_def.dialect,
            vars=vars,
            data_dir=self._data_dir,
        )

        with open(in_path, 'wb') as f:
            cloudpickle.dump(executor_payload, f)
        
        with open(log_file, 'wb') as log_fd:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=log_fd,
                stderr=subprocess.STDOUT)
            self._pending_execution[cell_id] = ExecutionState(
                process=proc,
                preamble_hash=self._preamble_hash,
                code_hash=cell_def.code_hash,
                vars=executor_payload.vars,
            )

        t = threading.Thread(target=self._wait_for_execution, args=[cell_id, out_path])
        t.start()
        return t

    def _wait_for_execution(self, cell_id, out_path):
        state = self._pending_execution[cell_id]
        exit_code = state.process.wait()
        
        if exit_code == 0:
            with open(out_path, 'rb') as f:
                result: ExecutorOutput = cloudpickle.load(f)

            with self._lock:
                os.rename(self._logs_dir / (cell_id + '.pending.log'),
                          self._logs_dir / (cell_id + '.current.log'))

                with self._db() as conn:
                    self._set_vars(conn, cell_id, result.created_vars)
                    self._set_deps(conn, cell_id,
                                   state,
                                   result.accessed_vars)
        else:
            log('executor finished with non-zero exit code (cell_id = %s)', cell_id)

        del self._pending_execution[cell_id]
        
                
    @_synchronized
    def _drop_cell(self, conn, cell_id):
        self._set_vars(conn, cell_id, {})
        conn.execute('delete from cell_state where id = ?', (cell_id,))
        conn.execute('delete from deps where cell_id = ?', (cell_id,))

    @_synchronized
    def _set_deps(self, conn, cell_id, state: ExecutionState, new_deps):
        current_vars = dict(conn.execute('select name, data_hash from vars').fetchall())
        dep_fresh = all( state.vars.get(dep) == current_vars.get(dep) for dep in new_deps )
        conn.execute('update cell_state set preamble_hash = ?, code_hash = ?, dep_fresh = ? where id = ?',
                     (self._preamble_hash, state.code_hash, dep_fresh, cell_id))

        conn.execute('delete from deps where cell_id = ?',
                     (cell_id,))

        conn.executemany('insert into deps (cell_id, dep_var_name) values (?, ?)',
                         [ (cell_id, dep) for dep in new_deps ])

    @_synchronized
    def _set_vars(self, conn, owner_cell, new_vars):
        all_other_vars = set(
            name for name, in 
            conn.execute('select name from vars where owner_cell <> ?', (owner_cell,)).fetchall() )

        if all_other_vars & new_vars.keys():
            # this would anyway fail due to unique index
            raise Exception('duplicate variable names with other cells: %s' % (all_other_vars & new_vars.keys()))
        
        # new_vars is dict name -> hash
        old_vars = conn.execute('select name, data_hash from vars where owner_cell = ?', (owner_cell,)).fetchall()
        old_vars = dict(old_vars)
        
        changes = [ (name,) for name in new_vars.keys() | old_vars.keys()
                    if old_vars.get(name) != new_vars.get(name)]
    
        conn.execute('delete from vars where owner_cell = ?', (owner_cell,))
        conn.executemany('insert into vars values (?, ?, ?)', [
            (owner_cell, name, hash)
            for name, hash in new_vars.items()
        ])
        conn.executemany('update cell_state set dep_fresh = false where id in (select cell_id from deps where dep_var_name = ?)', changes)

def internal_execute(payload_file, output_file):
    with open(payload_file, 'rb') as f:
        payload: ExecutorPayload = cloudpickle.load(f)
    os.unlink(payload_file)
        
    var_storage = VarStorage(payload.data_dir)
    
    output = payload.dialect.execute(
        payload.preamble_module,
        payload.code,
        payload.vars,
        var_storage)

    with atomic_open_for_writing(output_file, 'wb') as f:
        cloudpickle.dump(output, f)
