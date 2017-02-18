CREATE TABLE film (
    id BIGINT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    title VARCHAR(50) NOT NULL,
    description VARCHAR(255),
    release_year SMALLINT,
    rating VARCHAR(10)
);
