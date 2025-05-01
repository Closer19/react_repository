-- 1. madang.orders에 구매한 서적의 개수를 입력할 필드를 추가한다.(기본 값 1)
-- alter table orders change column amount count int default 1;
alter table orders add column count int default 1;

-- 2. madang.book에 각 서적에 대해 입고된 서적의 개수가 들어 갈 필드를 추가한다.(기본 값 100)
alter table book add column count int default 100;

-- 3. madang.customer에 고객등급을 입력할 필드를 추가한다.
alter table customer add column cust_grade varchar(10) default 'bronze';

-- 4. 고객등급변경내역 테이블을 생성한다.(일련번호,고객아이디, 변경날짜, 이전등급, 변경등급)
create table cust_grade_changeinfo (
id int auto_increment primary key, 
custid int,
change_date date,
pre_grade varchar(10),
update_grade varchar(10),
constraint custid_fk foreign key (custid) references customer(custid));

-- 5. 총 구입액을 기준(기준액 자유)으로 고객등급을 일괄처리하는 프로시저를 생성한다.(vip, gold, silver, bronze)
drop procedure if exists batch_update_grade;
delimiter //
create procedure batch_update_grade()	
begin
	declare new_grade varchar(10);
    declare old_grade varchar(10);
    DECLARE c_id varchar(10);
    DECLARE t_saleprice int;
    declare endofrow BOOL default false;
    
    declare usercursor cursor for
		select c.custid, sum(od.saleprice), c.cust_grade from customer c
        left outer join orders od on od.custid=c.custid group by c.custid;
	
    declare continue handler for not found
		set endofrow = true;
	
    open usercursor;
    
    cursor_loop:loop
		fetch usercursor into c_id, t_saleprice, old_grade;
        
        if endofrow then 
			leave cursor_loop;
		end if;
        
        case 
        when t_saleprice >=100000 then set new_grade='vip';
        when t_saleprice >=50000 then set new_grade='gold';
        when t_saleprice >=30000 then set new_grade='silver';
        else set new_grade='bronze';
        end case;
        
        if new_grade!= old_grade then
			update customer set cust_grade = new_grade where custid=c_id;
		end if;       
    end loop cursor_loop;
    close usercursor;    
end //
delimiter ;

-- 6. 고객등급이 변경되면 변경정보를 고객등급변경내역 테이블에 추가하는 트리거를 생성하고 고객등급을 일괄처리 프로시저를 실행한다.
drop TRIGGER if exists update_grade_trg;
delimiter //
create trigger update_grade_trg
after update on customer for each row
begin 
	insert into cust_grade_changeinfo values(null, old.custid, curdate(), old.cust_grade, new.cust_grade);	
end //
delimiter ;
call batch_update_grade();

-- 7. 고객이 서적을 주문 시 실행하는 프로시저를 생성한다.
--   - 고객 아이디, 구입할 책의 이름, 구매 개수를 입력(없는 서적에 대한 오류 처리 포함)
--   - saleprice는 고객의 등급에 따라 결정된다.
--   - order 테이블에 입력하고 주문이 입력되면 book테이블에 서적 개수를 감소시키는 트리거를 생성한다.(입력한 서적 개수보다 남은 서적 개수가 적으면 오류처리)
--   - 구매실적이 생길 때마다 해당 고객의 등급을 다시계산하여 업데이트하는 트리거를 생성한다.

drop procedure if exists orderbook;
delimiter //
create procedure orderbook( in c_id int, in b_name varchar(20), in cnt int)
begin
	declare b_id int;
    declare s_price int;
    declare c_grade varchar(10);
    declare b_price int;
    
    declare exit handler for not found
    begin
		select '없는 서적입니다';
        rollback;
	end;
    
    select bookid, price into b_id, b_price from book where bookname = b_name;
    select cust_grade into c_grade from customer where custid=c_id;
    
    case 
		when c_grade ='vip' then set s_price = (b_price * cnt) * 0.85;
		when c_grade ='gold' then set s_price = (b_price * cnt) * 0.90;
		when c_grade ='silver' then set s_price = (b_price * cnt) * 0.95;
		else set s_price = (b_price*cnt);
    end case;
    select c_id, b_id, s_price, cnt;
    insert into orders values (null, c_id, b_id, s_price, curdate(), cnt);  
    
end //
delimiter ;

drop TRIGGER if exists insert_order_trg;
delimiter //
create trigger insert_order_trg
before insert on orders for each row
begin 
	declare b_cnt int;
    
    select count into b_cnt from book where bookid = new.bookid;
    if b_cnt < new.count then 
		signal sqlstate '45000'
			set MESSAGE_TEXT = '충분한 도서 개수가 없습니다. ';
	end if;    
    
    update book set count = count-new.count where bookid=new.bookid;
end //
delimiter ;

drop TRIGGER if exists calculate_grade_trg;
delimiter //
create trigger calculate_grade_trg
after insert on orders for each row
begin 
	declare t_saleprice int;
    declare new_grade varchar(10);
    declare old_grade varchar(10);
    
    select sum(saleprice) into t_saleprice from orders where custid = new.custid;
    select cust_grade into old_grade from customer where custid = new.custid;
    
    case 
        when t_saleprice >=100000 then set new_grade='vip';
        when t_saleprice >=50000 then set new_grade='gold';
        when t_saleprice >=30000 then set new_grade='silver';
        else set new_grade='bronze';
    end case;
    
    if new_grade!= old_grade then
			update customer set cust_grade = new_grade where custid=new.custid;
	end if;       
    
end //
delimiter ;


















