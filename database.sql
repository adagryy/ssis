USE master
IF EXISTS(select * from sys.databases where name='recording_studio_db')
DROP DATABASE recording_studio_db

CREATE DATABASE recording_studio_db

USE recording_studio_db

CREATE TABLE ARTISTS(
	artist_id INT NOT NULL
		CONSTRAINT pk_artists PRIMARY KEY (artist_id),
	name varchar(60) NOT NULL
);

CREATE TABLE YEARS(
	year_id INT NOT NULL
		CONSTRAINT pk_years PRIMARY KEY(year_id),
	production_year INT NOT NULL
);

CREATE TABLE RECORDINGS(
	record_id INT NOT NULL
		CONSTRAINT pk_records PRIMARY KEY (record_id),
	year_id INT NOT NULL FOREIGN KEY REFERENCES YEARS (year_id),
	artist_id INT NOT NULL FOREIGN KEY REFERENCES ARTISTS (artist_id)
);

CREATE TABLE SONGS(
	song_id INT NOT NULL 
		CONSTRAINT pk_songs PRIMARY KEY (song_id),
	title varchar(60),
	lyrics TEXT NOT NULL,
	record_id INT NOT NULL FOREIGN KEY REFERENCES RECORDINGS(record_id)
);

CREATE TABLE USR(
	db_user_id int not null identity
		constraint pk_usr primary key,
	email nvarchar(100) NULL,
	_admin bit not null default 0
)
CREATE TABLE IMP
(
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

CREATE TABLE IMPORTES_ROWS(
	imp_id int not null CONSTRAINT FK_IR_IMP FOREIGN KEY REFERENCES IMP(imp_id),
	row_id int not null identity constraint PK_imported_rows PRIMARY KEY,
	import_status nvarchar(20) not null default 'not processed',
	master_id int null FOREIGN KEY REFERENCES RECORDINGS(record_id),
	name varchar(60) not null, 
	production_year INT NOT NULL,
	title varchar(60),
	lyrics TEXT NOT NULL
)

select * from IMPORTES_ROWS