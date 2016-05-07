USE master
IF EXISTS(select * from sys.databases where name='recording_studio_db')
DROP DATABASE recording_studio_db

CREATE DATABASE recording_studio_db

USE recording_studio_db

CREATE TABLE RECORDINGS(
	record_id INT NOT NULL identity
		CONSTRAINT pk_records PRIMARY KEY (record_id),
	year_of_record INT NOT NULL,
	artist_name varchar(60) NOT NULL 
)

CREATE TABLE SONGS(
	song_id INT NOT NULL IDENTITY
		CONSTRAINT pk_songs PRIMARY KEY (song_id),
	title varchar(60),
	lyrics TEXT NOT NULL,
	record_id INT NOT NULL FOREIGN KEY REFERENCES RECORDINGS(record_id)
)

CREATE TABLE USR(
	db_user_id int not null identity
		constraint pk_usr primary key,
	email nvarchar(100) NULL,
	_admin bit not null default 0
)

CREATE TABLE IMP(
	imp_id int not null IDENTITY CONSTRAINT PK_imp PRIMARY KEY, 
	start_dt datetime NOT NULL DEFAULT GETDATE(),
	end_dt datetime NULL /* jak nie null to sie zakonczyl */,
	err_no int NOT NULL DEFAULT 0,
	usr_nam nvarchar(100) not null DEFAULT USER_NAME(), 
	host nvarchar(100) not null DEFAULT HOST_NAME()
)

CREATE TABLE LOG_table(
	msg nvarchar(256) not null,
	proc_name nvarchar(100) null,
	step_name nvarchar(100) null,
    row_id int not null identity 
		CONSTRAINT PK_LOG PRIMARY KEY,
	entry_dt datetime not null DEFAULT GETDATE()
)

CREATE TABLE IMPORTED_ROWS(
	imp_id int not null CONSTRAINT FK_IR_IMP FOREIGN KEY REFERENCES IMP(imp_id),
	row_id int not null identity constraint PK_imported_rows PRIMARY KEY,
	import_status nvarchar(20) not null default 'not processed',
	master_id int null FOREIGN KEY REFERENCES RECORDINGS(record_id),
	name varchar(60) not null, 
	production_year INT NOT NULL,
	title varchar(60),
	lyrics TEXT NOT NULL
)

CREATE TABLE TMP(
	name varchar(60) not null, 
	production_year INT NOT NULL,
	title varchar(60),
	lyrics varchar(500) NOT NULL
)

CREATE TABLE PARAMETR(
	value_id INT NOT NULL identity 
		CONSTRAINT pk PRIMARY KEY,
	errors INT NOT NULL DEFAULT 0
)

use recording_studio_db


select SCOPE_IDENTITY(), name, production_year, title, lyrics from TMP
select *from IMPORTED_ROWS
insert into imp default values
insert into IMPORTED_ROWS (imp_id, name, production_year, title, lyrics) select SCOPE_IDENTITY(), name, production_year, title, lyrics from TMP

use recording_studio_db
go
CREATE PROCEDURE rewrite_from_TMP_to_IMPORTED 
AS
	INSERT INTO LOG_table (msg, proc_name, step_name) VALUES ('Transferring data to imported_rows started...', 'Success', 'Rewriting');
	INSERT INTO IMP DEFAULT VALUES;
	INSERT INTO IMPORTED_ROWS (imp_id, name, production_year, title, lyrics) SELECT SCOPE_IDENTITY(), name, production_year, title, lyrics FROM TMP;
	INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('IMP record inserted into correct table', 'Processing', 'Rewriting')
	INSERT INTO LOG_table (msg, proc_name, step_name) VALUES ('Transferring data to imported_rows finished...', 'Success', 'Rewriting');
GO

CREATE PROCEDURE process_data
AS
	INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Starting creating local variables', 'Processing', 'Creating local variables')
	DECLARE @I INT
	DECLARE @name varchar(60)
	DECLARE @production_year INT
	DECLARE @title varchar(60)
	DECLARE @lyrics varchar(1000)
	INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Local variables created', 'Processing', 'Creating local variables')
	DECLARE @record INT
	INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Cursor declaration', 'Processing', 'Creating local variables')
	declare kur SCROLL cursor for 
		select row_id from IMPORTED_ROWS WHERE import_status = 'not processed'
	OPEN kur;
	INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Cursor created', 'Processing', 'Creating cursor')
	FETCH NEXT FROM kur INTO @I;
		WHILE @@FETCH_STATUS=0
			BEGIN
			INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Fetching next record by cursor - SUCCESS', 'Processing', 'Looping through IR')
			SELECT @name = name FROM IMPORTED_ROWS 
				WHERE row_id = @I
			SELECT @production_year = production_year FROM IMPORTED_ROWS 
				WHERE row_id = @I
				IF( 
					SELECT artist_name FROM RECORDINGS
					WHERE (artist_name = @name) AND (year_of_record = @production_year)
					) IS NULL
					BEGIN
						INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Starting saving artists names and release years of albums', 'Processing', 'Saving')
						INSERT INTO RECORDINGS(artist_name, year_of_record) VALUES (@name, @production_year)
						UPDATE IMPORTED_ROWS SET master_id = (SELECT SCOPE_IDENTITY()) WHERE row_id = @I
						--====================
						SELECT @title = title FROM IMPORTED_ROWS 
							WHERE row_id = @I
						SELECT @lyrics = lyrics FROM IMPORTED_ROWS 
							WHERE row_id = @I
						IF(
							SELECT title FROM SONGS
							WHERE title = @title
							) IS NULL
							BEGIN
								SELECT @record = record_id FROM RECORDINGS
									WHERE (artist_name = @name) AND (year_of_record = @production_year)
								INSERT INTO SONGS (title, lyrics, record_id) VALUES (@title, @lyrics, @record)
								UPDATE IMPORTED_ROWS SET import_status = 'Processed' WHERE row_id = @I
								INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Import_status changed', 'Processing', 'Updating')
								INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Songs saved successfully', 'Processing', 'Saving')
							END
							ELSE
							BEGIN 
								UPDATE IMPORTED_ROWS SET import_status = 'Duplicated' WHERE row_id = @I
								INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Master_id updated', 'Processing', 'Updating')
								INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('ERROR - duplicate found', 'Processing', 'Saving')
							END
						--====================
					END
					ELSE --this happens, when there are songs with the same titles, but different artists
					BEGIN 
						--UPDATE IMPORTED_ROWS SET master_id=-1 WHERE row_id = @I
						INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Master_id updated', 'Processing', 'Updating')
						--====================
						SELECT @title = title FROM IMPORTED_ROWS 
							WHERE row_id = @I
						SELECT @lyrics = lyrics FROM IMPORTED_ROWS 
							WHERE row_id = @I
						IF(
							SELECT title FROM SONGS
							WHERE title = @title
							) IS NULL
							BEGIN
								SELECT @record = record_id FROM RECORDINGS
									WHERE (artist_name = @name) AND (year_of_record = @production_year)
								INSERT INTO SONGS (title, lyrics, record_id) VALUES (@title, @lyrics, @record)
								UPDATE IMPORTED_ROWS SET import_status = 'Processed' WHERE row_id = @I
								INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Import_status changed', 'Processing', 'Updating')
								INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Songs saved successfully', 'Processing', 'Saving')
							END
							ELSE
							BEGIN 
								UPDATE IMPORTED_ROWS SET import_status = 'Duplicated' WHERE row_id = @I
								INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('ERROR - duplicate found', 'Processing', 'Saving')
							END
						--====================
					END				
			FETCH NEXT FROM kur INTO @I
		END
	CLOSE kur   
	INSERT INTO LOG_table(msg, proc_name, step_name) VALUES ('Finalizing processing process', 'Processing', 'Finishing')
	DEALLOCATE kur
	UPDATE imp SET end_dt = GETDATE() WHERE end_dt IS NULL
GO

use recording_studio_db
select * from RECORDINGS
select * from TMP
select * from IMP
select * from LOG_table
select * from IMPORTED_ROWS

select * from RECORDINGS
select * from SONGS

select * FROM LOG_table
--SELECT  
DROP PROCEDURE process_data
--DROP PROCEDURE rewrite_from_TMP_to_IMPORTED
EXEC sp_MSForEachTable "DELETE FROM ?"

EXEC rewrite_from_TMP_to_IMPORTED
use recording_studio_db
EXEC process_data
go


sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
sp_configure 'Ole Automation Procedures', 1;
GO
RECONFIGURE;
GO

EXEC msdb.dbo.sysmail_add_account_sp
    @account_name = 'SendEmailSqlDemoAccount'
  , @description = 'Sending SMTP mails to users'
  , @email_address = 'heo2pu@gmail.com'
  , @display_name = 'Adam Gryczka'
  , @replyto_address = 'heo2pu@gmail.com'
  , @mailserver_name = 'smtp.gmail.com'
  , @port = 587
  , @username = 'heo2pu@gmail.com'
  , @password = ''
  , @enable_ssl = 1
Go


EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'Error_notification'
  , @recipients = 'deleter1234@interia.eu'
  , @subject = 'Ostrzezenie'
  , @body = 'Blad przy 2-krotnym wczytywaniu pliku'
  , @importance ='HIGH' 
GO

create procedure ssdd
as
	EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'Error_notification'
	  , @recipients = 'deleter1234@interia.eu'
	  , @subject = 'Ostrzezenie'
	  , @body = 'Blad przy 2-krotnym wczytywaniu pliku'
	  , @importance ='HIGH' 
	GO
go

DROP PROCEDURE ssdd
exec ssdd
