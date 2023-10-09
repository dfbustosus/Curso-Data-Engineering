SELECT a.name AS AgentName, cu.name AS CustomerName, x.duration
FROM
(
   SELECT ca.agentid, ca.duration, max(customerid) AS cid
   FROM
   (
       SELECT agentid, min(duration) as fastestcall
       FROM calls
       WHERE productsold = 1
       GROUP BY agentid
   ) min
   JOIN calls ca ON ca.agentid = min.agentid AND ca.duration = min.fastestcall
   WHERE productsold = 1
   GROUP BY ca.agentid, ca.duration
) x
JOIN agents a ON x.agentid = a.agentid
JOIN customers cu ON cu.customerid = x.cid
