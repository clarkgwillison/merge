# merge
 A tool for merging two directories

## Usage

    merge.py [-h] --db DB [--report] [--dedup] [--sync] [--move]
                    [--consolidate] [--absorb]
                    [dir_a] [dir_b]

    Analyze the contents of two directories to help merge them in the presence of
    additions, deletions, duplicates, and potential movements

    positional arguments:
      dir_a
      dir_b

    optional arguments:
      -h, --help     show this help message and exit
      --db DB        Name of the database file to use
      --report       Report all the differences found
      --dedup        Create a script to resolve duplicates
      --sync         Create a script to resolve differences (missing files)
      --move         When creating a sync script, make commands to move files not
                     copy them
      --consolidate  Create a script to consolidate files into dir_a
      --absorb       Create a script to absorb smaller dir_b into dir_a (faster
                     than consolidate)
