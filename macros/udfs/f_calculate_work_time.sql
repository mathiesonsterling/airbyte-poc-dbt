{% macro f_calculate_work_time() %}
CREATE OR REPLACE FUNCTION
{{target.schema}}.CalculateWorkTime(times Array<STRUCT<aTime TIMESTAMP, details STRING>>, endTime TIMESTAMP )
RETURNS  STRUCT<workTime int64, holdTime int64, workEnd TIMESTAMP>
LANGUAGE js AS r"""
function sumWorkTime(actions) {

  // "Calculation variables"
  let timeWorked = 0;
  let timeHeld = 0;
  let startTime = null;
  let startHoldTime = null;
  let state = "UNKNOWN"; // "UNKNOWN, ASSIGNED, PRODUCTIVE, UNASSIGNED";
  let holdState = "UNKNOWN"; // "UNKNOWN, RELEASED , ON_HOLD"

  // "Loop through actions"
  for (let i = 0; i < actions.length; i++) {
    if (!actions[i].details) {
      // "no way to tell what kind of action this is"
      continue;
    }
        let actionType = null; // "view, unassign, assign, productiveAction, holdAction, releaseAction, approve"

    if (actions[i].details.includes("viewed this")) {
      actionType = "view";
    } else if (times[i].aTime >= endTime) {
      // "using end time because a transaction can be unapproved and have multiple approved action details (endTime = time of last approval)."
      actionType = "approve"
    } else if (actions[i].details.includes("unassign")) {
      actionType = "unassign";
    } else if (actions[i].details.includes("assign")) {
      actionType = "assign";
    } else if (actions[i].details.includes("on hold")) {
      actionType = "holdAction";
    } else if (actions[i].details.includes("from hold")) {
      actionType = "releaseAction";
    } else {
      actionType = "productiveAction";
    }

    // "current action time"
    const actionTime = actions[i].aTime ? actions[i].aTime.getTime() : null;

    switch (state) {
      case "UNKNOWN":
        if (actionType === "assign") {
          startTime = actionTime;
          state = "ASSIGNED";
        }
        break;
      case "ASSIGNED":
        if (actionType === "assign") {
          startTime = actionTime;
          // no state change, stay Assigned
        }
        if (actionType === "productiveAction" || actionType === "holdAction" || actionType === "releaseAction") {
          state = "PRODUCTIVE";
        }
        if (actionType === "unassign") {
          state = "UNASSIGNED";
        }
        if (actionType === "approve") {
          if (actionTime) {
            timeWorked += (actionTime - startTime);
          }
        }
        break;
      case "PRODUCTIVE":
        if (actionType === "assign") {
          // "missing unassign action, don't add to timeWorked"
          startTime = actionTime;
          state = "ASSIGNED";
        }
        if (actionType === "unassign") {
          if (actionTime && startTime) {
            timeWorked += (actionTime - startTime);
          }
          state = "UNASSIGNED";
        }
        if (actionType === "approve") {
          if (actionTime) {
            timeWorked += (actionTime - startTime);
          }
        }
        break;
      case "UNASSIGNED":// "no state change, stay Assigned"
        if (actionType === "assign") {
          startTime = actionTime;
          state = "ASSIGNED";
        }
        break;
      default:
        throw new Error("Encountered undefined state: " + state);
    }

    // "Break out of loop on approve after adding time oon transaction approval."
    if (actionType === "approve") {
      break;
    }

    switch (holdState) {
      case "UNKNOWN":
        if (actionType === "holdAction") {
          startHoldTime = actionTime;
          holdState = "ON_HOLD";
        }
        break;
      case "ON_HOLD":
        if (actionType === "releaseAction") {
          if (actionTime) {
            timeHeld += (actionTime - startHoldTime);
          }
          holdState = "RELEASED";
        }
        break;
      case "RELEASED":
        if (actionType === "holdAction") {
          if (actionTime) {
            startHoldTime = actionTime;
          }
          holdState = "ON_HOLD";
        }
    }
  }
  return {
    workTime: Math.round(timeWorked / 60000.0),
    holdTime: Math.round(timeHeld / 60000.0),
    workEnd: endTime
  };
}
return sumWorkTime(times);
"""
{% endmacro %}