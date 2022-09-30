TABLES = {}
TABLES['user'] = (
    "CREATE TABLE `user` ("
    "  `id` VARCHAR(5) NOT NULL AUTO_INCREMENT,"
    "  `has_labels` BOOLEAN,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB"
)
TABLES['activity'] = (
    "CREATE TABLE `activity` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `user_id` VARCHAR(5),"
    "  `transportation_mode` VARCHAR(15),"
    "  `start_date_time` DATETIME,"
    "  `end_date_time` DATETIME,"
    "  PRIMARY KEY (`id`)"
    "  CONSTRAINT `user_id` FOREIGN KEY (`user_id`) "
    "     REFERENCES `user` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,"
    ") ENGINE=InnoDB"
)
TABLES['trackpoint'] = (
    "CREATE TABLE `trackpoint` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `lat` DOUBLE,"
    "  `lon` DOUBLE,"
    "  `altitude` int,"
    "  `lon` DOUBLE,"
    "  `date_days` DOUBLE,"
    "  `date_time` DATETIME,"
    "  PRIMARY KEY (`id`)"
    "  CONSTRAINT `activity_id` FOREIGN KEY (`activity_id`) "
    "     REFERENCES `activity` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,"
    ") ENGINE=InnoDB"
)
