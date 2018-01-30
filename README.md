# merge
 A tool for merging two directories

## Usage

    make_tables.py [-h] --db DB [--dedup] [--sync] [dir_a] [dir_b]

    positional arguments:
      dir_a
      dir_b

    optional arguments:
      -h, --help  show this help message and exit
      --db DB     Name of the database file to use
      --dedup     Create a script to resolve duplicates
      --sync      Create a script to resolve differences (missing files)
