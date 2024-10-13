# zimabook

persistent dataflow notebook

zimabook is a notebook (like Jupyter), but with the following properties:
 
 - all the state is kept on disk, so that your notebook can live forever
 - in cell runs in an isolated process, so you can execute multiple cells in parallel 
 - you can mark cells to be autoexecuted (if you want)
