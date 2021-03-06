# 29LeaksSQLSplitter
As far as the time of this writing, the 29 Leaks dump of Formations House contains a series of three tar'd
files each containing a MySQL dump file of a database of emails. This repository contains a few scripts to
make the analysis of these possible.

In total this processed ~50 million emails, which will be available soon in a fully searchable database. 
The process, when run end to end took about two weeks in total on a fairly bulky multi-core AWS instance.

## Issues To Contend With
There are a number issues that don't just let us import the files directly:
1. The files are only `ALTER TABLE`, so the database and tables have to be set up manually
1. The files after decompressing are **HUGE**. (552.1MB, 92.15GB, and 220.25GB)
1. Each import statement is not on a seperate line. Technically these files are like only 15 or 20 lines, mostly comments
1. Good luck finding a machine that can even handle a 220GB import file (since it'll be put into memory).
1. The 552MB file has some one `IMPORT` statement that is most of the file.
1. Examining these files in a regular text editor (or even something like `bat`) freezes basically any machine.
1. This means examining any formatting issues is a struggle.

There are additional issues with the data itself:
1. All the email headers are stored in a single column that is just a text string. @pudo has pointed out this is a PGP serialized string... because, sure.
1. It looks like there was a length limit in whatever system this came out of, because some of the headers seem to be cut off
1. Email messages can be made with a bunch of different encodings, but they were exported as, it seems, UTF-8 or extended ASCII. This means that anything that was in a non-Latin language is totally scrambled beyond repair.
1. The PHP seralize spec requires a "length of object", however, if it was scrambled this is invalid, and there's no way to guess how long it should be.
1. Instead I had to create, essentially, a fully Ruby, fault-tolerant, PHP serialize parser that handles malformed data properly, this may break and is pretty fragile, but works better than anyone should be able to hope for.

## Format

### Files
Each file contains a header setting up a bunch of database settings, followed by a ```LOCK TABLES `user_emails_archive` WRITE``` command.
The odd thing is that `user_emails_archive` is unused otherwise, but since it's here, I've left it. (Note that in the 552MB file this
first `LOCK TABLES` is not required.

Following the first one there's an `40000 ALTER TABLE` line that is apparently required, though, again that table is not used.

Immediately afterwards there is a ```LOCK TABLES `user_emails` WRITE;```

Below this are the `INSERT` statements.

### INSERT Statements
Each insert statment contains five columns:
- id (integer)
- plain-text content (string)
- html content (string)
- headers (serialized PHP as a string)
- text-encoding (string)

## Relevant Files In This Repository
There are a few different files. All of them have a `--help` to explain their options.
The main ones that are used are as follows:
- `sql_split.rb`
  - A program that runs through an SQL dump, splitting it into files either of a specified size or with a specified number of lines.
- `mysql_management.rb`
  - A program to take a folder of split SQL files and to import them to a MySQL database. (**Note:** The database and tables have to be set up manually ahead of time.
- `email_cleaner.rb`
  - A program to parse the headers of records in a MySQL database after they were imported and export the emails as a `.eml` file.
  - This does a few things
  	1. Extracts the "to" and "from" and puts them into the table.
	1. Saves a more easily parsed JSON version of the headers to the database as well
	1. Filters for SPAM using the spam score assigned in the database
	1. Exports as a properly formatted `.eml` file.
  - **NOTE:** Make sure to manually add the `to` and `from` columns to the database after importing, but before running this.

## Running

### Requirements
- Ruby 2.6.3
- mysql2 working (this can get tricky, I recommend installing this gem seperately before `bundle install`
- Running MySQL instance on a machine big enough to handle the import sizes. At least a TB.
- Probably a big machine in general otherwise this can take a *very* long time.

### Setup

1. First, make sure you have the most recent version of Ruby installed. I tend to use [https://rvm.io/](rvm).
1. Clone this repository to somewhere.
1. Install the package manager (we don't have many, but it's nice). `gem install bundler`
1. Install the required packages. `bundle install`

### Splitting Files
1. Make sure you have the unarchived dump files somewhere nearby you can access
1. `ruby sql_split.rb -s 50 ../sql_dumps/user_emails.sql` will split the file into 50mb chunks
   - Alternatively, `ruby sql_split.rb -l 40 ../sql_dumps/user_emails.sql` will split it into 40 imports per file.
   - This saves everything to a `output` folder in the home directory of the sql file. **NOTE:** This will overwrite anything already in the folder.
   - If you're splitting one of the `archive` files you have to add a `-a` flag to manage the headers properly. `ruby sql_split.rb -s 50 -a .....`

### Importing Files
1. First make sure you split the files (duh)
1. `ruby mysql_management.rb -u <database-user> -p<database-password> <output folder> <database-name>` will import them into the database indicated.

### Formatting the Database (Parsing Headers)
1. Make sure the import worked (duh)
1. `ruby email_cleaner.rb -u <database-user> -p<database-password> <database-name>` will go through, parse the headers, and save everything
   - I've put some multi-threading in here, hopefully it will help speed up the process of parsing. It's all in the `-h` but here's some basics
	 - `-s` The number of threads (default 5)
	 - `--mysql-timeout` Probably only useful if you're doing this with a remote database, if it's local this shouldn't be touched
	 - `-v` Prints out stuff
	 - `--debug` Sets debug mode, turning off concurrency and making verbose true. Good if you need to debug the parser since debuggers and concurrency don't play well together.

### Exporting the New Database to .eml Files
1. Also make sure everything is imported
1. `ruby email_cleaner.rb -u <mysql-database-user> -p <mysql-database-password> -h <myslq-host-name> -e <eml-output-directory>`
    - There's some more here as well, `-h` should explain them but:
         - `-i` The id to start from when getting entries from the db, for partial output
	 - `-v` verbose mode, etc.
	 - `--debug` puts everything into single thread mode and lets you debug
1. Wait... a *very* long time, like, days, weeks. The end will be a `.eml` directly. From here you can zip them (another tedious process) or whatever.

## Contact
This was created by Christopher Guess [@cguess](https://www.twitter.com/cguess).
If you want more info reach out to him at [cguess@gmail.com](mailto:cguess@gmail.com)
PGP Key: https://keybase.io/cguess
