# Copyright (c) 2013 Georgios Gousios
# MIT-licensed

create database stackoverflow DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;

use stackoverflow;

create table badges (
  Id INT NOT NULL PRIMARY KEY,
  UserId INT,
  Name VARCHAR(50),
  CreationDate DATETIME
);

CREATE TABLE comments (
    Id INT NOT NULL PRIMARY KEY,
    PostId INT NOT NULL,
    Score INT NOT NULL DEFAULT 0,
    Text TEXT,
    CreationDate DATETIME,
    UserId INT NOT NULL
);

CREATE TABLE post_history (
    Id INT NOT NULL PRIMARY KEY,
    PostHistoryTypeId SMALLINT NOT NULL,
    PostId INT NOT NULL,
    RevisionGUID VARCHAR(36),
    CreationDate DATETIME,
    UserId INT NOT NULL,
    Text TEXT
);

CREATE TABLE posts (
    Id INT NOT NULL PRIMARY KEY,
    PostTypeId SMALLINT,
    AcceptedAnswerId INT,
    ParentId INT,
    Score INT NULL,
    ViewCount INT NULL,
    Body text NULL,
    OwnerUserId INT NOT NULL,
    LastEditorUserId INT,
    LastEditDate DATETIME,
    LastActivityDate DATETIME,
    Title varchar(256) NOT NULL,
    Tags VARCHAR(256),
    AnswerCount INT NOT NULL DEFAULT 0,
    CommentCount INT NOT NULL DEFAULT 0,
    FavoriteCount INT NOT NULL DEFAULT 0,
    CreationDate DATETIME
);

CREATE TABLE users (
    Id INT NOT NULL PRIMARY KEY,
    Reputation INT NOT NULL,
    CreationDate DATETIME,
    DisplayName VARCHAR(50) NULL,
    LastAccessDate  DATETIME,
    Views INT DEFAULT 0,
    WebsiteUrl VARCHAR(256) NULL,
    Location VARCHAR(256) NULL,
    AboutMe TEXT NULL,
    Age INT,
    UpVotes INT,
    DownVotes INT,
    EmailHash VARCHAR(32)
);

CREATE TABLE votes (
    Id INT NOT NULL PRIMARY KEY,
    PostId INT NOT NULL,
    VoteTypeId SMALLINT,
    CreationDate DATETIME
);

load xml infile 'Badges.xml'
into table badges
rows identified by '<row>';

load xml infile 'Comments.xml'
into table comments
rows identified by '<row>';

load xml infile 'PostHistory.xml'
into table post_history
rows identified by '<row>';

load xml infile 'Posts.xml'
into table posts
rows identified by '<row>';

load xml infile 'Users.xml'
into table users
rows identified by '<row>';

load xml infile 'Votes.xml'
into table votes
rows identified by '<row>';

create index badges_idx_1 on badges(UserId);

create index comments_idx_1 on comments(PostId);
create index comments_idx_2 on comments(UserId);

create index post_history_idx_1 on post_history(PostId);
create index post_history_idx_2 on post_history(UserId);

create index posts_idx_1 on posts(AcceptedAnswerId);
create index posts_idx_2 on posts(ParentId);
create index posts_idx_3 on posts(OwnerUserId);
create index posts_idx_4 on posts(LastEditorUserId);

create index votes_idx_1 on votes(PostId);

SHOW DATABASES;

show TABLES;
use datasciencese;

show tables;
CREATE TABLE badges (
    Id INT PRIMARY KEY,
    UserId INT NOT NULL,
    Name VARCHAR(255),
    Date DATETIME(3),
    Class INT,
    TagBased BOOLEAN
);

create index badges_idx_1 on badges(UserId);

SET GLOBAL local_infile=1;

LOAD XML LOCAL INFILE '/Users/joyjitchoudhury/Downloads/test/datascience.stackexchange.com/Badges.xml'
INTO TABLE badges
ROWS IDENTIFIED BY '<row>'
SET
    Id = @Id,
    UserId = @UserId,
    Name = @Name,
    Date = STR_TO_DATE(@Date, '%Y-%m-%dT%H:%i:%s.%f'),
    Class = @Class,
    TagBased = IF(@TagBased = 'True', TRUE, FALSE);

DELETE FROM USERS;


CREATE TABLE users (
    Id INT PRIMARY KEY,
    Reputation INT NOT NULL,
    CreationDate DATETIME(3) NOT NULL,
    DisplayName VARCHAR(255) NOT NULL,
    LastAccessDate DATETIME(3),
    WebsiteUrl VARCHAR(255),
    Location VARCHAR(255),
    AboutMe TEXT,
    Views INT,
    UpVotes INT,
    DownVotes INT,
    AccountId INT
);

select count(*) from usersv2;


select count(*) from users where Location is not null;


CREATE TABLE usersv2 LIKE users;
INSERT INTO usersv2 SELECT * FROM users;

alter table usersv2
    add lat DECIMAL(11,8) null;

alter table usersv2
    add `long` DECIMAL(11,8) null;

alter table usersv2
    add `location_addresstype` VARCHAR(30);

alter table usersv2
    add `location_display_name` VARCHAR(200);

SELECT Id, Location
FROM usersv2
WHERE Location IS NOT NULL
  AND lat IS NULL
  AND `long` IS NULL;


SELECT count(Id)
FROM usersv2
WHERE Location IS NOT NULL
  AND lat IS NULL
  AND `long` IS NULL;

SELECT count(*)
FROM usersv2
WHERE Location IS NOT NULL;

ALTER TABLE usersv2
ADD COLUMN loc_geoloc_na TINYINT(1) DEFAULT 0;

ALTER TABLE usersv2
ADD COLUMN loc_revgeoloc_na TINYINT(1) DEFAULT 0;

SELECT count(*)
FROM usersv2
WHERE usersv2.loc_geoloc_na = 1;

update usersv2 set usersv2.loc_geoloc_na = 0;

SELECT COUNT(*)
FROM usersv2
WHERE Location IS NOT NULL
  AND loc_geoloc_na = 0;


SELECT COUNT(*)
FROM usersv2
WHERE Location IS NOT NULL
  AND loc_geoloc_na = 0
  AND lat IS NOT NULL
  AND `long` IS NOT NULL;

CREATE TABLE usersLocation (
    user_id INT PRIMARY KEY,
    lat DECIMAL(10, 7),
    lon DECIMAL(10, 7),
    suburb VARCHAR(255),
    city VARCHAR(255),
    county VARCHAR(255),
    state VARCHAR(255),
    ISO3166_2_lvl4 VARCHAR(10),
    country VARCHAR(255),
    country_code CHAR(2),
    loc_revgeoloc_na TINYINT(1) DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES usersv2(Id)
);


CREATE TABLE location_country_mapping (
    id INT PRIMARY KEY AUTO_INCREMENT,
    location VARCHAR(255),
    country_code VARCHAR(10)
);

CREATE INDEX idx_location ON location_country_mapping(location);

SELECT u.Id AS user_id, u.lat, u.long
FROM usersv2 u
JOIN usersLocation ul ON u.Id = ul.user_id
WHERE u.Location IS NOT NULL
  AND u.lat IS NOT NULL
  AND u.long IS NOT NULL
  AND (ul.loc_revgeoloc_na = 0 OR ul.loc_revgeoloc_na IS NULL);


SELECT u.Id AS user_id, u.lat, u.long
FROM usersv2 u
LEFT JOIN usersLocation ul ON u.Id = ul.user_id
WHERE u.Location IS NOT NULL
  AND u.lat IS NOT NULL
  AND u.long IS NOT NULL
  AND (ul.loc_revgeoloc_na = 0 OR ul.loc_revgeoloc_na IS NULL OR ul.user_id IS NULL);


select * from usersLocation where user_id > 108086;
select * from usersv2 where Id = 103380;

create DATABASE datasciencese;

SHow databases;

select count(*) from users;

CREATE TABLE `location_country_mapping` (
  `id` int NOT NULL AUTO_INCREMENT,
  `location` varchar(255) DEFAULT NULL,
  `country_code` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_location` (`location`)
) ENGINE=InnoDB AUTO_INCREMENT=10531 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `users` (
  `Id` int NOT NULL,
  `Reputation` int NOT NULL,
  `CreationDate` datetime(3) NOT NULL,
  `DisplayName` varchar(255) NOT NULL,
  `LastAccessDate` datetime(3) DEFAULT NULL,
  `WebsiteUrl` varchar(255) DEFAULT NULL,
  `Location` varchar(255) DEFAULT NULL,
  `AboutMe` text,
  `Views` int DEFAULT NULL,
  `UpVotes` int DEFAULT NULL,
  `DownVotes` int DEFAULT NULL,
  `AccountId` int DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `badges` (
  `Id` int NOT NULL,
  `UserId` int NOT NULL,
  `Name` varchar(255) DEFAULT NULL,
  `Date` datetime(3) DEFAULT NULL,
  `Class` int DEFAULT NULL,
  `TagBased` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `badges_idx_1` (`UserId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `userslocation` (
  `user_id` int NOT NULL,
  `lat` decimal(10,7) DEFAULT NULL,
  `lon` decimal(10,7) DEFAULT NULL,
  `suburb` varchar(255) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL,
  `county` varchar(255) DEFAULT NULL,
  `state` varchar(255) DEFAULT NULL,
  `ISO3166_2_lvl4` varchar(10) DEFAULT NULL,
  `country` varchar(255) DEFAULT NULL,
  `country_code` char(2) DEFAULT NULL,
  `loc_revgeoloc_na` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`user_id`),
  CONSTRAINT `userslocation_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `usersv2` (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `usersv2` (
  `Id` int NOT NULL,
  `Reputation` int NOT NULL,
  `CreationDate` datetime(3) NOT NULL,
  `DisplayName` varchar(255) NOT NULL,
  `LastAccessDate` datetime(3) DEFAULT NULL,
  `WebsiteUrl` varchar(255) DEFAULT NULL,
  `Location` varchar(255) DEFAULT NULL,
  `AboutMe` text,
  `Views` int DEFAULT NULL,
  `UpVotes` int DEFAULT NULL,
  `DownVotes` int DEFAULT NULL,
  `AccountId` int DEFAULT NULL,
  `lat` decimal(11,8) DEFAULT NULL,
  `long` decimal(11,8) DEFAULT NULL,
  `location_addresstype` varchar(30) DEFAULT NULL,
  `location_display_name` varchar(200) DEFAULT NULL,
  `loc_geoloc_na` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;