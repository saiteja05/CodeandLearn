SELECT    Btrim(querytxt) AS txt,
          q.query,
          i.initial_query_time,
          w.service_class_end_time,
          i.wlm_query_memory,
          i.is_short_query,
          i.seldispatch_status,
          clevel,
          cache_type,
          segments,
          Date_diff('ms',initial_query_time,service_class_start_time)/1000 ::   decimal AS pre_wlm,
          total_queue_time                                           /1000000 ::decimal AS queue_s,
          c.ctime                                                                       AS compile_s,
          ((w.total_exec_time/1000000)::decimal - c.ctime::decimal )::decimal              exec_only_time,
          v.segment_execution_time                                                         max_seg_time,
          date_diff('ms',initial_query_time,service_class_end_time)/1000 ::decimal      AS elapsed,
          b.scanned_blocks                                                              AS scannedblks,
          nvl(transfer_mb,0)                                                            AS remote_io,
          s3api_cnt,
          b.total_blocks                   AS totalblks,
          nvl(query_temp_blocks_to_disk,0) AS spill,
          v.io_skew,
          v.cpu_skew,
          v.query_cpu_usage_percent AS cpu
FROM      stl_query q
LEFT JOIN stl_wlm_query
using     (query)
          (
                   SELECT   query ,
                            sum(date_diff('ms',starttime,endtime))/1000 ::decimal AS ctime,
                            max(skip_opt_compile)                                 AS short_compiled ,
                            max(complevel)                                        AS clevel,
                            count(segment)                                        AS segments,
                            max(cachehit)                                         AS cache_type
                   FROM     stl_compile_info
                   WHERE    userid>1
                   GROUP BY 1) c
using     (query)
LEFT JOIN
          (
                   SELECT   query,
                            count(*)                     AS s3api_cnt,
                            sum(transfer_size)/1000/1000 AS transfer_mb
                   FROM     stl_s3client
                   WHERE    http_method LIKE 'GET%'
                   GROUP BY query) s
using     (query)
LEFT JOIN svl_query_metrics_summary v
using     (query)
LEFT JOIN stl_internal_query_details i
using     (query)
LEFT JOIN
          (
                   SELECT   query,
                            sum(num_blocks_to_scan) AS scanned_blocks ,
                            sum(total_blocks)       AS total_blocks
                   FROM     stl_scan_range_stats
                   GROUP BY 1) b
using     (query)
WHERE     q.userid>=100
AND       q.starttime >='< TS ex :2022-10-31 00:00>'
AND       q.querytxt ilike '%<pattern you want to search>%'
AND       elapsed>5
ORDER BY  1,
          3;