package com.bvan.film;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author bvanchuhov
 */
public class FilmDBRunner {

    private static final String SQL_GET_ALL = "SELECT * FROM film";
    private static final String SQL_GET_BY_ID = "SELECT * FROM film WHERE id=?";

    public static void main(String[] args) throws SQLException {
        DataSource dataSource = createPooledDataSource();

        Film filmById = findFilmById(dataSource, 10);
        System.out.println("Film with id=10: " + filmById);
        System.out.println();

        List<Film> films = findAllFilms(dataSource);
        System.out.println("All films:");
        int count = 1;
        for (Film film : films) {
            System.out.println(count++ + ": " + film);
        }
    }

    private static List<Film> findAllFilms(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();

            try (ResultSet resultSet = statement.executeQuery(SQL_GET_ALL)) {
                return getFilms(resultSet);
            }
        }
    }

    private static Film findFilmById(DataSource dataSource, long id) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(SQL_GET_BY_ID);

            preparedStatement.setLong(1, id);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                return (resultSet.next()) ? getFilm(resultSet) : null;
            }
        }
    }

    private static List<Film> getFilms(ResultSet resultSet) throws SQLException {
        List<Film> films = new ArrayList<>();
        while (resultSet.next()) {
            Film film = getFilm(resultSet);
            films.add(film);
        }
        return films;
    }

    private static Film getFilm(ResultSet resultSet) throws SQLException {
        Film film = new Film();
        film.setId(resultSet.getLong("id"));
        film.setTitle(resultSet.getString("title"));
        film.setDescription(resultSet.getString("description"));
        film.setReleaseYear(resultSet.getInt("release_year"));
        film.setRating(Rating.valueOf(resultSet.getString("rating")));
        return film;
    }


    private static DataSource createPooledDataSource() {
        Properties properties = loadDBProperties();

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(properties.getProperty("com.mysql.jdbc.Driver"));
        dataSource.setUrl(properties.getProperty("db.url"));
        dataSource.setUsername(properties.getProperty("db.user"));
        dataSource.setPassword(properties.getProperty("db.password"));

        dataSource.setInitialSize(5);
        dataSource.setMinIdle(5);
        dataSource.setMaxIdle(20);
        dataSource.setMaxOpenPreparedStatements(10);

        return dataSource;
    }

    private static DataSource createSimpleDataSource() {
        Properties props = loadDBProperties();

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(props.getProperty("db.url"));
        dataSource.setUser(props.getProperty("db.user"));
        dataSource.setPassword(props.getProperty("db.password"));

        return dataSource;
    }

    private static Connection createConnection() throws SQLException {
        Properties props = loadDBProperties();

        String url = props.getProperty("db.url");
        String user = props.getProperty("db.user");
        String password = props.getProperty("db.password");

        return DriverManager.getConnection(url, user, password);
    }

    private static Properties loadDBProperties() {
        Properties properties = new Properties();
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream("db.properties"))) {
            properties.load(inputStream);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
