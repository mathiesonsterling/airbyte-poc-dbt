{% macro f_determine_transaction_end_time() %}
-- Function to find the last approved action time for the time worked calculation.
CREATE OR REPLACE FUNCTION
{{target.schema}}.DetermineTransactionEndTime(times Array<STRUCT<aTime TIMESTAMP, details STRING>>)
RETURNS  timestamp
LANGUAGE js AS r"""
function findFulfilled(times) {
  let fulfillTime = times[0].atime;


  for (let i = 0; i < times.length; i++){
    const currDetails = times[i].details;
     if (!times[i].aTime){
        continue;
    }
    if (currDetails.includes("approve") ){
      fulfillTime = times[i].aTime;
    }
  }

  return fulfillTime;
}
return findFulfilled(times);
"""
{% endmacro %}