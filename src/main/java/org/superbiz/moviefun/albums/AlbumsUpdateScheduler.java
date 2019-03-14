package org.superbiz.moviefun.albums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Configuration
@EnableAsync
@EnableScheduling
public class AlbumsUpdateScheduler {
    private DataSource dataSource;
    private static final long SECONDS = 1000;
    private static final long MINUTES = 60 * SECONDS;

    private final AlbumsUpdater albumsUpdater;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private LocalDateTime localDateTime = LocalDateTime.now();
    private JdbcTemplate jdbcTemplate;

    public AlbumsUpdateScheduler(AlbumsUpdater albumsUpdater,DataSource dataSource) {
        this.albumsUpdater = albumsUpdater;
        this.dataSource = dataSource;
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private LocalDateTime fromSql;

    @Scheduled(initialDelay = 15 * SECONDS, fixedRate = 2 * MINUTES)
    public void run() {
        try {
            if(startAlbumSchedulerTask()) {
                logger.debug("Starting albums update");
                Timestamp timestamp = Timestamp.valueOf(localDateTime);
                Timestamp ts = Timestamp.valueOf(fromSql);
                String query1 = "Insert into album_scheduler_task (started_at) Values(?)";
                String query = "Update album_scheduler_task set started_at = ? where started_at = ?";
                jdbcTemplate.update(connection -> {
                    PreparedStatement preparedStatement = connection.prepareStatement(query);
                    try{
                        preparedStatement.setTimestamp(1,timestamp);
                        preparedStatement.setTimestamp(2,ts);
                    }catch (SQLException e){
                        System.out.println(e.getMessage());
                    }
                    return preparedStatement;
                });
                logger.debug("Successfully updated the table");
                albumsUpdater.update();
                logger.debug("Finished albums update");
            }else{
                logger.debug("Nothing to start");
            }
        } catch (Throwable e) {
            logger.error("Error while updating albums", e);
        }
    }

    private boolean startAlbumSchedulerTask() {

        String query="Select * from album_scheduler_task order by started_at desc Limit 1";
        fromSql = jdbcTemplate.queryForObject(query, LocalDateTime.class);
        logger.debug("Performed the select query");
//        if(fromSql == null){
//            return true;
//        }
        long difference = ChronoUnit.MINUTES.between(fromSql,localDateTime);
        if(difference > 2 || fromSql == null){
            logger.debug("I will be returning true");
            return true;
        }
        return false;
//        int updatedRows = jdbcTemplate.update(
//                "UPDATE album_scheduler_task" +
//                        " SET started_at = now()" +
//                        " WHERE started_at IS NULL" +
//                        " OR started_at < date_sub(now(), INTERVAL 2 MINUTE)"
//        );
//
//        return updatedRows > 0;
    }
}
