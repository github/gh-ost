CREATE DATABASE IF NOT EXISTS `test`;

CREATE USER IF NOT EXISTS `gh-ost`@`%`;
SET PASSWORD FOR `gh-ost`@`%` = PASSWORD('gh-ost');
GRANT ALL ON *.* TO `gh-ost`@`%`;
