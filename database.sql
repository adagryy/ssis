USE master
IF EXISTS(select * from sys.databases where name='recording_studio_db')
DROP DATABASE recording_studio_db

CREATE DATABASE recording_studio_db

USE recording_studio_db

-- CREATE TABLE ARTISTS(
-- 	artist_id INT NOT NULL identity
-- 		CONSTRAINT pk_artists PRIMARY KEY (artist_id),
-- 	name varchar(60) NOT NULL
-- )

-- CREATE TABLE YEARS(
-- 	year_id INT NOT NULL identity
-- 		CONSTRAINT pk_years PRIMARY KEY(year_id),
-- 	production_year INT NOT NULL
-- )

-- CREATE TABLE RECORDINGS(
-- 	record_id INT NOT NULL identity
-- 		CONSTRAINT pk_records PRIMARY KEY (record_id),
-- 	year_id INT NOT NULL FOREIGN KEY REFERENCES YEARS (year_id),
-- 	artist_id INT NOT NULL FOREIGN KEY REFERENCES ARTISTS (artist_id)
-- )

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

use recording_studio_db


select SCOPE_IDENTITY(), name, production_year, title, lyrics from TMP
select *from IMPORTED_ROWS
insert into imp default values
insert into IMPORTED_ROWS (imp_id, name, production_year, title, lyrics) select SCOPE_IDENTITY(), name, production_year, title, lyrics from TMP

use recording_studio_db
go
CREATE PROCEDURE rewrite_from_TMP_to_IMPORTED 
AS
	INSERT INTO LOG_table (msg, proc_name, step_name) VALUES ('Transferring data to imported_rows started...', 'Success', 'Row');
	INSERT INTO IMP DEFAULT VALUES;
	INSERT INTO IMPORTED_ROWS (imp_id, name, production_year, title, lyrics) SELECT SCOPE_IDENTITY(), name, production_year, title, lyrics FROM TMP;
	INSERT INTO LOG_table (msg, proc_name, step_name) VALUES ('Transferring data to imported_rows finished...', 'Success', 'Row');
GO

CREATE PROCEDURE process_data
AS
	INSERT INTO LOG_table (msg, proc_name, step_name) VALUES ('Processing loaded data started...', 'Success', 'Row');
	DECLARE @I INT
	DECLARE @name varchar(60)
	DECLARE @production_year INT
	DECLARE @title varchar(60)
	DECLARE @lyrics varchar(1000)

	DECLARE @record INT
	declare kur SCROLL cursor for 
		select row_id from IMPORTED_ROWS WHERE import_status = 'not processed'
	OPEN kur;
	FETCH NEXT FROM kur INTO @I;
		WHILE @@FETCH_STATUS=0
			BEGIN
			SELECT @name = name FROM IMPORTED_ROWS 
				WHERE row_id = @I
			SELECT @production_year = production_year FROM IMPORTED_ROWS 
				WHERE row_id = @I
				IF( 
					SELECT artist_name FROM RECORDINGS
					WHERE (artist_name = @name) AND (year_of_record = @production_year)
					) IS NULL
					BEGIN
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
							END
							ELSE
							BEGIN 
								UPDATE IMPORTED_ROWS SET import_status = 'Duplicated' WHERE row_id = @I
							END
						--====================
					END
					ELSE
					BEGIN 
						UPDATE IMPORTED_ROWS SET master_id=-1 WHERE row_id = @I
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
							END
							ELSE
							BEGIN 
								UPDATE IMPORTED_ROWS SET import_status = 'Duplicated' WHERE row_id = @I
							END
						--====================
					END				
			FETCH NEXT FROM kur INTO @I
		END
	CLOSE kur   
	DEALLOCATE kur
	UPDATE imp SET end_dt = GETDATE() WHERE end_dt IS NULL
GO

use recording_studio_db
select * from RECORDINGS
select * from TMP
select * from IMP
select * from LOG_table
select * from IMPORTED_ROWS

select * FROM LOG_table

select * from SONGS

--SELECT  
--DROP PROCEDURE process_data
--DROP PROCEDURE rewrite_from_TMP_to_IMPORTED


EXEC rewrite_from_TMP_to_IMPORTED
use recording_studio_db
EXEC process_data
go

TRUNCATE TABLE IMPORTED_ROWS
TRUNCATE TABLE SONGS
-- UPDATE IMPORTED_ROWS SET master_id=4 WHERE name = 'Adele'

-- 		IF (	SELECT s.title 
-- 					FROM SONGS s
-- 					JOIN RECORDINGS r ON r.record_id = s.song_id
-- 					JOIN IMPORTED_ROWS ir ON ir.master_id = r.record_id 
-- 					WHERE s.title = ir.title
-- 				) IS NULL
-- 			BEGIN				
-- 				INSERT INTO SONGS (title, lyrics, record_id) VALUES (
-- 				(SELECT title FROM IMPORTED_ROWS WHERE row_id = @I ),
-- 				(SELECT lyrics FROM IMPORTED_ROWS WHERE row_id = @I ),
-- 				(SELECT title FROM IMPORTED_ROWS WHERE row_id = @I )
-- 				)
-- 			END

-- 		IF(
-- 			SELECT r.year_of_record, r.artist_name FROM RECORDINGS r
-- 			JOIN IMPORTED_ROWS ir2 ON ir2.master_id = r.record_id
-- 			WHERE r.year_of_record = ir2.production_year AND (r.artist_name = ir2.name)
-- 		) IS NULL
-- 		BEGIN
-- 			INSERT INTO RECORDINGS (year_of_record, artist_name) VALUES ((SELECT production_year FROM IMPORTED_ROWS WHERE row_id = @I), (SELECT name FROM IMPORTED_ROWS WHERE row_id = @I))
-- 			UPDATE IMPORTED_ROWS SET master_id = (SELECT IDENT_CURRENT('RECORDINGS')) WHERE row_id = @I

