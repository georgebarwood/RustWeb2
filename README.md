Webserver based on [rustdb](https://github.com/georgebarwood/RustDB) database, 
with database browsing, password hashing, database replication, email transmission and timed jobs.

Installation and starting server
================================
First install [Rust](https://www.rust-lang.org/tools/install) if it is not already installed.
Then install rustweb2 from a command prompt using

cargo install rustweb2

From a command prompt, change to the the directory where the database is to be stored ( the file will be named rustweb.rustdb ). 
Start rustweb2 using

rustweb2 3000

This should start rustweb2 server, listening on port 3000 ( you can use any available port ).
You should then be able to browse to http://localhost:3000/admin
From there are links to a Manual, Execute SQL, a list of Schemas and other links.

Security
========

Initially login security is disabled. To enable it 

(1) Edit the function login.hash and change the salt string.

(2) Use the Logins Menu link to add a login user and set password.

(3) Edit the function login.user ( see instructions included there ).

Initialisation
==============
A new database is initialised from the file admin-ScriptAll.txt in the current directory.

If ScriptAll.txt does not exist a default initialisation is used.

Database replication
====================

Start Rustweb2 in the directory (folder) where you want the replicated database stored, specifying the  -rep option

For example:

rustweb2 2000 --rep https://mydomain.com

If login security has been enabled, you will need to specify login details ( obtained from the login.user table ), for example:

--login "uid=1; hpw=0xaaa023850abbdff839894888dd8e8abbceaaa023855abbdff839894888dd8e8c"

If the database is very large, it may be more practical to use FTP to get an initial copy of the database, otherwise a copy will be fetched automatically.

Replication is enabled by records being inserted in the log.Transaction table. 

These records can be periodically deleted, provided that all replication servers are up to date.

Note: starting from version 1.1.1 (March 2024) transaction records are not applied until log.Roll() is executed. 
This means that in the event of an accident (such as an incorrect drop, update or delete statement) the database can be recovered by omitting the faulty transaction in log.Roll().

Email
=====

Email can be sent using the email schema.

(1) Create a record in email.SmtpServer

(2) Create an email in email.msg

(3) Insert it into email.Queue

(4) Call the builtin function EMAILTX()

If an email cannot be sent, and the error is temporary, it will be inserted into the email.Delayed table and retried later.

Permanent errors are logged in email.SendError

Timed Jobs
==========

A named SQL function (with no parameters) can be called at a specified time by creating a record in timed.Job.

This is used by the email system to retry temporary email send errors.

Read Only Requests
==================

GET requests are processed using a read-only copy of the database, any changes made are not saved.
This is useful for requests that take a significant time to process, as other requests can be processed in parallel.
This can be overriden by adding a query parameter "save".

POST requests are assumed to be read-write, this can be overridden by adding a query parameter "readonly".

Optional Features
=================
mt-tokio enables the multi-threaded tokio runtime (enabled by default).

unsafe-optim enables unsafe optimisations (should not cause any problem, but programmers are fallible).

For example this will install rustweb2 with unsafe optimisations enabled:

cargo install rustweb2 -F unsafe-optim

Links
=====

crates.io : https://crates.io/crates/rustweb2

repository: https://github.com/georgebarwood/Rustweb2

blog: https://rustdb.wordpress.com/
