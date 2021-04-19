drop table if exists gh_ost_test;
create table gh_ost_test (
                             id int auto_increment,
                             i int not null,
                             e enum('red', 'blue', 'orange', 'green') null default null collate 'utf8_bin',
                             primary key(id)
) auto_increment=1;

drop event if exists gh_ost_test;
delimiter ;;
create event gh_ost_test
    on schedule every 1 second
        starts current_timestamp
        ends current_timestamp + interval 60 second
    on completion not preserve
    enable
    do
    begin
        insert into gh_ost_test values (null, 11, 1);
        insert into gh_ost_test values (null, 13, 4);
        insert into gh_ost_test values (null, 17, 1);
        set @last_insert_id := last_insert_id();
        update gh_ost_test set e=4 where id = @last_insert_id;
        insert into gh_ost_test values (null, 23, null);
        set @last_insert_id := last_insert_id();
        update gh_ost_test set i=i+1, e=null where id = @last_insert_id;
    end ;;