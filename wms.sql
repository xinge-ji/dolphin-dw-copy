create or replace procedure p_create_workload_dtl(p_begindate varchar2,p_enddate varchar2) is

  v_begindate varchar2(100);
  v_enddate   varchar2(100);

  v_sqlcode   varchar2(100);
  v_sqlerrm   varchar2(1000);
  m_count number;
  m_dtlid number;

  --WMS平库入库明细
  cursor c_zx_19001_v is
    select trunc(t.rffindate) as credate,
           t.rfman as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.wholecount, 0)) as wholecount,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19001_v t
     where t.rffindate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.rffindate), t.rfman;
    r_zx_19001_v c_zx_19001_v%rowtype;
  
  --IWCS×Ô¶¯¿âÈë¿âÃ÷Ï¸²éÑ¯
  cursor c_zx_19007_v is
    select trunc(t.credate) as credate,
           t.scaner as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.wholecount, 0)) as wholecount,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19007_v t
     where t.credate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
     group by trunc(t.credate), t.scaner;
    r_zx_19007_v c_zx_19007_v%rowtype;

  --WMSÆ½¿â¼ð»õÃ÷Ï¸²éÑ¯
  cursor c_zx_19002_v is
    select trunc(t.rffindate) as credate,
           t.rfman as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.wholecount, 0)) as wholecount,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19002_v t
     where t.rffindate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.rffindate), t.rfman;
    r_zx_19002_v c_zx_19002_v%rowtype;
    
  --IWCS×Ô¶¯¿â¼ð»õÃ÷Ï¸²éÑ¯
  cursor c_zx_19008_v is
    select trunc(t.credate) as credate,
           t.picker as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.wholecount, 0)) as wholecount,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19008_v t
     where t.credate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
     group by trunc(t.credate), t.picker;
    r_zx_19008_v c_zx_19008_v%rowtype;
    
  --WMSÆ½¿â³ö¿â¸´ºËÃ÷Ï¸²éÑ¯
  cursor c_zx_19003_v is
    select trunc(t.keepdate) as credate,
           t.checkman as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.wholecount, 0)) as wholecount,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19003_v t
     where t.keepdate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.keepdate), t.checkman;
    r_zx_19003_v c_zx_19003_v%rowtype;
    
  --IWCS×Ô¶¯¿â³ö¿â¸´ºËÃ÷Ï¸²éÑ¯
  cursor c_zx_19009_v is
    select trunc(t.checkdate) as credate,
           t.checker as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.wholecount, 0)) as wholecount,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19009_v t
     where t.checkdate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
     group by trunc(t.checkdate), t.checker;
    r_zx_19009_v c_zx_19009_v%rowtype;

  --WMSÆ½¿â²¹»õÃ÷Ï¸²éÑ¯
  cursor c_zx_19004_v is
    select trunc(t.rffindate) as credate,
           t.rfman as employeename,
           sum(nvl(t.upcount, 0)) as upcount,
           sum(nvl(t.downcount, 0)) as downcount
      from zx_19004_v t
     where t.rffindate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.rffindate), t.rfman;
    r_zx_19004_v c_zx_19004_v%rowtype;

  --WMSÆ½¿âÒÆÎ»Ã÷Ï¸²éÑ¯
  cursor c_zx_19005_v is
    select trunc(t.rffindate) as credate,
           t.rfman as employeename,
           sum(nvl(t.upcount, 0)) as upcount,
           sum(nvl(t.downcount, 0)) as downcount
      from zx_19005_v t
     where t.rffindate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.rffindate), t.rfman;
    r_zx_19005_v c_zx_19005_v%rowtype;    

  --IWCS×Ô¶¯¿â²¹»õÒÆ¿âÉÏ¼ÜÃ÷Ï¸²éÑ¯
  cursor c_zx_19010_v is
    select trunc(t.credate) as credate,
           t.scaner as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.wholecount, 0)) as wholecount,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19010_v t
     where t.credate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
     group by trunc(t.credate), t.scaner;
    r_zx_19010_v c_zx_19010_v%rowtype;
    
  --IWCS×Ô¶¯¿âµ¹ÏäÃ÷Ï¸²éÑ¯
  cursor c_zx_19011_v is
    select trunc(t.credate) as credate,
           t.scaner as employeename,
           sum(nvl(t.countqty, 0)) as countqty
      from zx_19011_v t
     where t.credate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
     group by trunc(t.credate), t.scaner;
    r_zx_19011_v c_zx_19011_v%rowtype;
    
  --IWCS×Ô¶¯¿âµØ¶ÑÉÏ¼ÜÃ÷Ï¸²éÑ¯
  cursor c_zx_19012_v is
    select trunc(t.credate) as credate,
           t.scaner as employeename,
           sum(nvl(t.countqty, 0)) as countqty,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19012_v t
     where t.credate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
     group by trunc(t.credate), t.scaner;
    r_zx_19012_v c_zx_19012_v%rowtype;
    
  --IWCS×Ô¶¯¿âÒÆ¿â³öÃ÷Ï¸²éÑ¯
  cursor c_zx_19013_v is
    select trunc(t.credate) as credate,
           t.checker as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.wholecount, 0)) as wholecount,
           sum(nvl(t.wholeqty, 0)) as wholeqty
      from zx_19013_v t
     where t.credate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
     group by trunc(t.credate), t.checker;
    r_zx_19013_v c_zx_19013_v%rowtype;
    
  --WMSÖÐÒ©Èë¿âÃ÷Ï¸²éÑ¯
  cursor c_zx_19014_v is
    select trunc(t.rffindate) as credate,
           t.rfman as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.kgqty, 0)) as kgqty
      from zx_19014_v t
     where t.rffindate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.rffindate), t.rfman;
    r_zx_19014_v c_zx_19014_v%rowtype;
    
  --WMSÖÐÒ©¼ð»õÃ÷Ï¸²éÑ¯
  cursor c_zx_19015_v is
    select trunc(t.rffindate) as credate,
           t.rfman as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.kgqty, 0)) as kgqty
      from zx_19015_v t
     where t.rffindate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.rffindate), t.rfman;
    r_zx_19015_v c_zx_19015_v%rowtype;
    
  --WMSÖÐÒ©³ö¿â¸´ºËÃ÷Ï¸²éÑ¯
  cursor c_zx_19016_v is
    select trunc(t.keepdate) as credate,
           t.checkman as employeename,
           sum(nvl(t.scattercount, 0)) as scattercount,
           sum(nvl(t.kgqty, 0)) as kgqty
      from zx_19016_v t
     where t.keepdate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.keepdate), t.checkman;
    r_zx_19016_v c_zx_19016_v%rowtype;
    
  --WMSÖÐÒ©ÒÆÎ»Ã÷Ï¸²éÑ¯
  cursor c_zx_19017_v is
    select trunc(t.rffindate) as credate,
           t.rfman as employeename,
           sum(nvl(t.upcount, 0)) as upcount,
           sum(nvl(t.downcount, 0)) as downcount,
           sum(decode(t.inoutflag,0,t.kgqty,0)) as upkgqty,
           sum(decode(t.inoutflag,1,t.kgqty,0)) as downkgqty
      from zx_19017_v t
     where t.rffindate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.rffindate), t.rfman;
    r_zx_19017_v c_zx_19017_v%rowtype;
    
  --WMSµç×Ó¼à¹ÜÂë²É¼¯²éÑ¯
  cursor c_zx_19006_v is
    select trunc(t.credate) as credate,
           t.inputman as employeename,
           count(1) as ecodeqty
      from zx_19006_wmsecode_v t
     where t.credate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.credate), t.inputman,t.inoutflag;
    r_zx_19006_v c_zx_19006_v%rowtype;
    
  --IWCSµç×Ó¼à¹ÜÂë²É¼¯²éÑ¯
  cursor c_zx_19018_v is
    select trunc(t.credate) as credate,
           t.scaner as employeename,
           count(1) as ecodeqty
      from zx_19018_v t
     where t.credate between
           to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss')
    group by trunc(t.credate), t.scaner;
    r_zx_19018_v c_zx_19018_v%rowtype;        
begin
  --³õÊ¼»¯Ê±¼ä·¶Î§
  select p_begindate||' 00:00:00' into v_begindate from dual;
  select p_enddate||' 23:59:59' into v_enddate from dual;
  --ÅÐ¶ÏÈÕÆÚ·¶Î§ÄÚÊÇ·ñÓÐ¼ÇÂ¼´æÔÚ£¬ÓÐµÄ»°Ö±½ÓÉ¾³ý¼ÇÂ¼£¬ÖØÐÂÉú³É
  select count(1)
    into m_count
    from wms_workload_dtl t
   where t.credate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
         to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss');
         
  if m_count<>0 then
    delete from wms_workload_dtl
     where credate between to_date(v_begindate, 'yyyy-mm-dd hh24:mi:ss') and
           to_date(v_enddate, 'yyyy-mm-dd hh24:mi:ss');
  end if;
  
  --WMSÆ½¿âÈë¿âÃ÷Ï¸²éÑ¯£¬Í³¼ÆWMSÆ½¿âÉÏ¼ÜÉ¢¼þÌõÄ¿Êý£¬Õû¼þÌõÊýÄ¿£¬Õû¼þ¼þÊý
  open c_zx_19001_v;
  loop
    fetch c_zx_19001_v
     into r_zx_19001_v;
    exit when c_zx_19001_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19001_v.credate
       and t.employeename = r_zx_19001_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         wms_in_scattercount,
         wms_in_wholecount,
         wms_in_wholeqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19001_v.credate credate,
               sysdate execdate,
               r_zx_19001_v.employeename employeename,
               r_zx_19001_v.scattercount wms_in_scattercount,
               r_zx_19001_v.wholecount wms_in_wholecount,
               r_zx_19001_v.wholeqty wms_in_wholeqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19001_v.credate
         and t.employeename = r_zx_19001_v.employeename;
          
      update wms_workload_dtl t
         set t.wms_in_scattercount = r_zx_19001_v.scattercount,
             t.wms_in_wholecount   = r_zx_19001_v.wholecount,
             t.wms_in_wholeqty     = r_zx_19001_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19001_v;
  
  --IWCS×Ô¶¯¿âÈë¿âÃ÷Ï¸²éÑ¯£¬Í³¼ÆIWCS×Ô¶¯¿âÉÏ¼ÜÉ¢¼þÌõÄ¿Êý£¬Õû¼þÌõÊýÄ¿£¬Õû¼þ¼þÊý
  open c_zx_19007_v;
  loop
    fetch c_zx_19007_v
     into r_zx_19007_v;
    exit when c_zx_19007_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19007_v.credate
       and t.employeename = r_zx_19007_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         iwcs_in_scattercount,
         iwcs_in_wholecount,
         iwcs_in_wholeqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19007_v.credate         credate,
               sysdate                      execdate,
               r_zx_19007_v.employeename    employeename,
               r_zx_19007_v.scattercount    iwcs_in_scattercount,
               r_zx_19007_v.wholecount      iwcs_in_wholecount,
               r_zx_19007_v.wholeqty        iwcs_in_wholeqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19007_v.credate
         and t.employeename = r_zx_19007_v.employeename;
      
      update wms_workload_dtl t
         set t.iwcs_in_scattercount = r_zx_19007_v.scattercount,
             t.iwcs_in_wholecount   = r_zx_19007_v.wholecount,
             t.iwcs_in_wholeqty     = r_zx_19007_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19007_v;
  
  --WMSÆ½¿â¼ð»õÃ÷Ï¸²éÑ¯£¬Í³¼ÆWMSÆ½¿â¼ð»õÉ¢¼þÌõÄ¿Êý£¬Õû¼þÌõÊýÄ¿£¬Õû¼þ¼þÊý
  open c_zx_19002_v;
  loop
    fetch c_zx_19002_v
     into r_zx_19002_v;
    exit when c_zx_19002_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19002_v.credate
       and t.employeename = r_zx_19002_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         wms_out_scattercount,
         wms_out_wholecount,
         wms_out_wholeqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19002_v.credate credate,
               sysdate execdate,
               r_zx_19002_v.employeename employeename,
               r_zx_19002_v.scattercount wms_out_scattercount,
               r_zx_19002_v.wholecount wms_out_wholecount,
               r_zx_19002_v.wholeqty wms_out_wholeqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19002_v.credate
         and t.employeename = r_zx_19002_v.employeename;
          
      update wms_workload_dtl t
         set t.wms_out_scattercount = r_zx_19002_v.scattercount,
             t.wms_out_wholecount   = r_zx_19002_v.wholecount,
             t.wms_out_wholeqty     = r_zx_19002_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19002_v;
  
  --IWCS×Ô¶¯¿â¼ð»õÃ÷Ï¸²éÑ¯£¬Í³¼ÆIWCS×Ô¶¯¿â¼ð»õÉ¢¼þÌõÄ¿Êý£¬Õû¼þÌõÊýÄ¿£¬Õû¼þ¼þÊý
  open c_zx_19008_v;
  loop
    fetch c_zx_19008_v
     into r_zx_19008_v;
    exit when c_zx_19008_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19008_v.credate
       and t.employeename = r_zx_19008_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         iwcs_out_scattercount,
         iwcs_out_wholecount,
         iwcs_out_wholeqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19008_v.credate         credate,
               sysdate                      execdate,
               r_zx_19008_v.employeename    employeename,
               r_zx_19008_v.scattercount    wms_out_scattercount,
               r_zx_19008_v.wholecount      wms_out_wholecount,
               r_zx_19008_v.wholeqty        wms_out_wholeqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19008_v.credate
         and t.employeename = r_zx_19008_v.employeename;
      
      update wms_workload_dtl t
         set t.iwcs_out_scattercount = r_zx_19008_v.scattercount,
             t.iwcs_out_wholecount   = r_zx_19008_v.wholecount,
             t.iwcs_out_wholeqty     = r_zx_19008_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19008_v;
  
  --WMSÆ½¿â³ö¿â¸´ºËÃ÷Ï¸²éÑ¯£¬Í³¼ÆWMSÆ½¿â³ö¿â¸´ºËÉ¢¼þÌõÄ¿Êý£¬Õû¼þÌõÊýÄ¿£¬Õû¼þ¼þÊý
  open c_zx_19003_v;
  loop
    fetch c_zx_19003_v
     into r_zx_19003_v;
    exit when c_zx_19003_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19003_v.credate
       and t.employeename = r_zx_19003_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         wms_check_scount,
         wms_check_wcount,
         wms_check_wqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19003_v.credate credate,
               sysdate execdate,
               r_zx_19003_v.employeename employeename,
               r_zx_19003_v.scattercount wms_check_scount,
               r_zx_19003_v.wholecount wms_check_wcount,
               r_zx_19003_v.wholeqty wms_check_wqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19003_v.credate
         and t.employeename = r_zx_19003_v.employeename;
          
      update wms_workload_dtl t
         set t.wms_check_scount = r_zx_19003_v.scattercount,
             t.wms_check_wcount   = r_zx_19003_v.wholecount,
             t.wms_check_wqty     = r_zx_19003_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19003_v;
  
  --IWCS×Ô¶¯¿â³ö¿â¸´ºËÃ÷Ï¸²éÑ¯£¬Í³¼ÆIWCS×Ô¶¯¿â³ö¿â¸´ºËÉ¢¼þÌõÄ¿Êý£¬Õû¼þÌõÊýÄ¿£¬Õû¼þ¼þÊý
  open c_zx_19009_v;
  loop
    fetch c_zx_19009_v
     into r_zx_19009_v;
    exit when c_zx_19009_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19009_v.credate
       and t.employeename = r_zx_19009_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         iwcs_check_scount,
         iwcs_check_wcount,
         iwcs_check_wqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19009_v.credate         credate,
               sysdate                      execdate,
               r_zx_19009_v.employeename    employeename,
               r_zx_19009_v.scattercount    iwcs_check_scount,
               r_zx_19009_v.wholecount      iwcs_check_wcount,
               r_zx_19009_v.wholeqty        iwcs_check_wqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19009_v.credate
         and t.employeename = r_zx_19009_v.employeename;
      
      update wms_workload_dtl t
         set t.iwcs_check_scount = r_zx_19009_v.scattercount,
             t.iwcs_check_wcount   = r_zx_19009_v.wholecount,
             t.iwcs_check_wqty     = r_zx_19009_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19009_v;
  
  --WMSÆ½¿â²¹»õÃ÷Ï¸²éÑ¯£¬Í³¼ÆWMSÆ½¿â²¹»õÉÏ¼ÜÌõÊýºÍÏÂ¼ÜÌõÊý
  open c_zx_19004_v;
  loop
    fetch c_zx_19004_v
     into r_zx_19004_v;
    exit when c_zx_19004_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19004_v.credate
       and t.employeename = r_zx_19004_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         wms_replenish_up,
         wms_replenish_down)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19004_v.credate         credate,
               sysdate                      execdate,
               r_zx_19004_v.employeename    employeename,
               r_zx_19004_v.upcount         wms_replenish_up,
               r_zx_19004_v.downcount       wms_replenish_down
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19004_v.credate
         and t.employeename = r_zx_19004_v.employeename;
          
      update wms_workload_dtl t
         set t.wms_replenish_up   = r_zx_19004_v.upcount,
             t.wms_replenish_down = r_zx_19004_v.downcount
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19004_v;
  
  --WMSÆ½¿âÒÆÎ»Ã÷Ï¸²éÑ¯£¬Í³¼ÆWMSÆ½¿â²¹»õÉÏ¼ÜÌõÊýºÍÏÂ¼ÜÌõÊý
  open c_zx_19005_v;
  loop
    fetch c_zx_19005_v
     into r_zx_19005_v;
    exit when c_zx_19005_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19005_v.credate
       and t.employeename = r_zx_19005_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         wms_mv_up,
         wms_mv_down)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19005_v.credate         credate,
               sysdate                      execdate,
               r_zx_19005_v.employeename    employeename,
               r_zx_19005_v.upcount         wms_mv_up,
               r_zx_19005_v.downcount       wms_mv_down
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19005_v.credate
         and t.employeename = r_zx_19005_v.employeename;
          
      update wms_workload_dtl t
         set t.wms_mv_up   = r_zx_19005_v.upcount,
             t.wms_mv_down = r_zx_19005_v.downcount
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19005_v;
  
  --IWCS×Ô¶¯¿â²¹»õÒÆ¿âÉÏ¼ÜÃ÷Ï¸²éÑ¯£¬Í³¼Æ´ÓWMS²¹»õ»òÊÖ¹¤ÒÆÎ»µ½IWCSÏµÍ³µÄÉÏ¼ÜÉ¢¼þÌõÊý£¬Õû¼þÌõÊý£¬Õû¼þ¼þÊý
  open c_zx_19010_v;
  loop
    fetch c_zx_19010_v
     into r_zx_19010_v;
    exit when c_zx_19010_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19010_v.credate
       and t.employeename = r_zx_19010_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         iwcs_transfer_scount,
         iwcs_transfer_wcount,
         iwcs_transfer_wqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19010_v.credate         credate,
               sysdate                      execdate,
               r_zx_19010_v.employeename    employeename,
               r_zx_19010_v.scattercount    iwcs_transfer_scount,
               r_zx_19010_v.wholecount      iwcs_transfer_wcount,
               r_zx_19010_v.wholeqty        iwcs_transfer_wqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19010_v.credate
         and t.employeename = r_zx_19010_v.employeename;
      
      update wms_workload_dtl t
         set t.iwcs_transfer_scount = r_zx_19010_v.scattercount,
             t.iwcs_transfer_wcount   = r_zx_19010_v.wholecount,
             t.iwcs_transfer_wqty     = r_zx_19010_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19010_v;
  
  --IWCS×Ô¶¯¿âµ¹ÏäÃ÷Ï¸²éÑ¯£¬Í³¼ÆIWCSµÄµ¹ÏäÌõÄ¿Êý
  open c_zx_19011_v;
  loop
    fetch c_zx_19011_v
     into r_zx_19011_v;
    exit when c_zx_19011_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19011_v.credate
       and t.employeename = r_zx_19011_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid, credate, execdate, employeename, iwcs_boxtransfer_count)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19011_v.credate         credate,
               sysdate                      execdate,
               r_zx_19011_v.employeename    employeename,
               r_zx_19011_v.countqty        iwcs_boxtransfer_count
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19011_v.credate
         and t.employeename = r_zx_19011_v.employeename;
      
      update wms_workload_dtl t
         set t.iwcs_boxtransfer_count = r_zx_19011_v.countqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19011_v;
  
  --IWCS×Ô¶¯¿âµØ¶ÑÉÏ¼ÜÃ÷Ï¸²éÑ¯£¬Í³¼ÆIWCSµØ¶ÑÆ½¿âµÄÉÏ¼ÜÈÎÎñÌõÄ¿ÊýºÍÕû¼þ¼þÊý
  open c_zx_19012_v;
  loop
    fetch c_zx_19012_v
     into r_zx_19012_v;
    exit when c_zx_19012_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19012_v.credate
       and t.employeename = r_zx_19012_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         iwcs_groundup_count,
         iwcs_groundup_wqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19012_v.credate         credate,
               sysdate                      execdate,
               r_zx_19012_v.employeename    employeename,
               r_zx_19012_v.countqty        iwcs_groundup_count,
               r_zx_19012_v.wholeqty        iwcs_groundup_wqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19012_v.credate
         and t.employeename = r_zx_19012_v.employeename;
      
      update wms_workload_dtl t
         set t.iwcs_groundup_count = r_zx_19012_v.countqty,
             t.iwcs_groundup_wqty  = r_zx_19012_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19012_v;
  
  --IWCS×Ô¶¯¿âÒÆ¿â³öÃ÷Ï¸²éÑ¯£¬Í³¼ÆIWCS×Ô¶¯¿âÒÆ¿âµ½WMSÆ½¿âµÄÉ¢¼þÌõÊý¡¢Õû¼þÌõÊý¡¢Õû¼þ¼þÊý
  open c_zx_19013_v;
  loop
    fetch c_zx_19013_v
     into r_zx_19013_v;
    exit when c_zx_19013_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19013_v.credate
       and t.employeename = r_zx_19013_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         iwcs_mvout_scount,
         iwcs_mvout_wcount,
         iwcs_mvout_wqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19013_v.credate         credate,
               sysdate                      execdate,
               r_zx_19013_v.employeename    employeename,
               r_zx_19013_v.scattercount    iwcs_mvout_scount,
               r_zx_19013_v.wholecount      iwcs_mvout_wcount,
               r_zx_19013_v.wholeqty        iwcs_mvout_wqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19013_v.credate
         and t.employeename = r_zx_19013_v.employeename;
      
      update wms_workload_dtl t
         set t.iwcs_mvout_scount = r_zx_19013_v.scattercount,
             t.iwcs_mvout_wcount   = r_zx_19013_v.wholecount,
             t.iwcs_mvout_wqty     = r_zx_19013_v.wholeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19013_v;
  
  --WMSÖÐÒ©Èë¿âÃ÷Ï¸²éÑ¯£¬Í³¼ÆWMSÖÐÒ©ÉÏ¼ÜÉ¢¼þÌõÄ¿Êý£¬¹«½ïÊý
  open c_zx_19014_v;
  loop
    fetch c_zx_19014_v
     into r_zx_19014_v;
    exit when c_zx_19014_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19014_v.credate
       and t.employeename = r_zx_19014_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         tcm_in_scattercount,
         tcm_in_kgqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19014_v.credate         credate,
               sysdate                      execdate,
               r_zx_19014_v.employeename    employeename,
               r_zx_19014_v.scattercount    tcm_in_scattercount,
               r_zx_19014_v.kgqty           tcm_in_kgqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19014_v.credate
         and t.employeename = r_zx_19014_v.employeename;
          
      update wms_workload_dtl t
         set t.tcm_in_scattercount = r_zx_19014_v.scattercount,
             t.tcm_in_kgqty        = r_zx_19014_v.kgqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19014_v;
  
  --WMSÖÐÒ©¼ð»õÃ÷Ï¸²éÑ¯£¬Í³¼ÆWMSÖÐÒ©¼ð»õÉ¢¼þÌõÄ¿Êý£¬¹«½ïÊý
  open c_zx_19015_v;
  loop
    fetch c_zx_19015_v
     into r_zx_19015_v;
    exit when c_zx_19015_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19015_v.credate
       and t.employeename = r_zx_19015_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         tcm_out_scattercount,
         tcm_out_kgqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19015_v.credate         credate,
               sysdate                      execdate,
               r_zx_19015_v.employeename    employeename,
               r_zx_19015_v.scattercount    tcm_out_scattercount,
               r_zx_19015_v.kgqty           tcm_out_kgqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19015_v.credate
         and t.employeename = r_zx_19015_v.employeename;
          
      update wms_workload_dtl t
         set t.tcm_out_scattercount = r_zx_19015_v.scattercount,
             t.tcm_out_kgqty        = r_zx_19015_v.kgqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19015_v;
  
  --WMSÖÐÒ©³ö¿â¸´ºËÃ÷Ï¸²éÑ¯£¬Í³¼ÆWMSÖÐÒ©³ö¿â¸´ºËÉ¢¼þÌõÄ¿Êý£¬¹«½ïÊý
  open c_zx_19016_v;
  loop
    fetch c_zx_19016_v
     into r_zx_19016_v;
    exit when c_zx_19016_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19016_v.credate
       and t.employeename = r_zx_19016_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         tcm_check_sqty,
         tcm_check_kgqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19016_v.credate         credate,
               sysdate                      execdate,
               r_zx_19016_v.employeename    employeename,
               r_zx_19016_v.scattercount    tcm_check_sqty,
               r_zx_19016_v.kgqty           tcm_check_kgqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19016_v.credate
         and t.employeename = r_zx_19016_v.employeename;
          
      update wms_workload_dtl t
         set t.tcm_check_sqty  = r_zx_19016_v.scattercount,
             t.tcm_check_kgqty = r_zx_19016_v.kgqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19016_v;
  
  --WMSÖÐÒ©ÒÆÎ»Ã÷Ï¸²éÑ¯£¬Í³¼ÆWMSÖÐÒ©ÒÆÎ»ÉÏ¼ÜÌõÊýºÍÏÂ¼ÜÌõÊý
  open c_zx_19017_v;
  loop
    fetch c_zx_19017_v
     into r_zx_19017_v;
    exit when c_zx_19017_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19017_v.credate
       and t.employeename = r_zx_19017_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid,
         credate,
         execdate,
         employeename,
         tcm_mv_up,
         tcm_mv_down,
         tcm_mv_up_kgqty,
         tcm_mv_down_kgqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19017_v.credate         credate,
               sysdate                      execdate,
               r_zx_19017_v.employeename    employeename,
               r_zx_19017_v.upcount         tcm_mv_up,
               r_zx_19017_v.downcount       tcm_mv_down,
               r_zx_19017_v.upkgqty         tcm_mv_up_kgqty,
               r_zx_19017_v.downkgqty       tcm_mv_down_kgqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19017_v.credate
         and t.employeename = r_zx_19017_v.employeename;
      
      update wms_workload_dtl t
         set t.tcm_mv_up         = r_zx_19017_v.upcount,
             t.tcm_mv_down       = r_zx_19017_v.downcount,
             t.tcm_mv_up_kgqty   = r_zx_19017_v.upkgqty,
             t.tcm_mv_down_kgqty = r_zx_19017_v.downkgqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19017_v;
  
  --WMSµç×Ó¼à¹ÜÂë²éÑ¯£¬Í³¼ÆWMSÏµÍ³²É¼¯µÄµç×Ó¼à¹ÜÂëÌõÊý
  open c_zx_19006_v;
  loop
    fetch c_zx_19006_v
     into r_zx_19006_v;
    exit when c_zx_19006_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19006_v.credate
       and t.employeename = r_zx_19006_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid, credate, execdate, employeename, ecodeqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19006_v.credate         credate,
               sysdate                      execdate,
               r_zx_19006_v.employeename    employeename,
               r_zx_19006_v.ecodeqty        ecodeqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19006_v.credate
         and t.employeename = r_zx_19006_v.employeename;
      
      update wms_workload_dtl t
         set t.ecodeqty = t.ecodeqty + r_zx_19006_v.ecodeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19006_v;
  
  --IWCSµç×Ó¼à¹ÜÂë²éÑ¯£¬Í³¼ÆIWCSÏµÍ³²É¼¯µÄµç×Ó¼à¹ÜÂëÌõÊý
  open c_zx_19018_v;
  loop
    fetch c_zx_19018_v
     into r_zx_19018_v;
    exit when c_zx_19018_v%notfound;
    --ÅÐ¶Ï¼ÇÂ¼ÊÇ·ñ´æÔÚ
    select count(1)
      into m_count
      from wms_workload_dtl t
     where trunc(t.credate) = r_zx_19018_v.credate
       and t.employeename = r_zx_19018_v.employeename;
    if m_count=0 then 
      --²»´æÔÚ¾Í²åÈë¼ÇÂ¼
      insert into wms_workload_dtl
        (dtlid, credate, execdate, employeename, ecodeqty)
        select wms_workload_dtl_seq.nextval dtlid,
               r_zx_19018_v.credate         credate,
               sysdate                      execdate,
               r_zx_19018_v.employeename    employeename,
               r_zx_19018_v.ecodeqty        ecodeqty
          from dual;
    else
      --´æÔÚ¾Í¸üÐÂ¼ÇÂ¼
      select t.dtlid
        into m_dtlid
        from wms_workload_dtl t
       where trunc(t.credate) = r_zx_19018_v.credate
         and t.employeename = r_zx_19018_v.employeename;
      
      update wms_workload_dtl t
         set t.ecodeqty = t.ecodeqty + r_zx_19018_v.ecodeqty
       where t.dtlid = m_dtlid;
    end if; 
  end loop;
  close c_zx_19018_v;
  commit; 
exception
  when others then
    rollback;
    v_sqlcode := SQLCODE;
    v_sqlerrm := SQLERRM;
  commit;
end;
