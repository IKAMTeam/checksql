package com.onevizion.checksql;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;

public class ExceptionUtils {

    public final static String START_ERR_MSG_TAG = "<ERRORMSG>";
    public final static String END_ERR_MSG_TAG = "</ERRORMSG>";

    public static String getUserFriendlyErrMessage(Exception e) {
        Throwable e1;
        String msg;
        if (EmptyResultDataAccessException.class.equals(e.getClass())) {
            e1 = e;
            msg = "No data found";
        } else if (DataAccessException.class.isInstance(e) && e.getCause() != null) {
            // hide SQL text from user
            e1 = e.getCause();
            msg = e1.getMessage();
        } else if (ArrayIndexOutOfBoundsException.class.isInstance(e)) {
            e1 = e;
            msg = null;
        } else if (DeadlockLoserDataAccessException.class.isInstance(e)) {
            e1 = e;
            msg = "Resource is currently busy, please try again in few minutes";
        } else {
            e1 = e;
            msg = e1.getMessage();
        }
        if (msg == null) {
         // trying to get user friendly message splitting exception class
            // name to the words by upper case chars
            msg = StringUtils.join(e1.getClass().getSimpleName().split("(?=[A-Z])"), " ");
        }

        String errMsg = cutErrorMsgInTags(msg);
        if (errMsg != null) {
            msg = errMsg;
        }

        return msg;
    }

    public static String cutErrorMsgInTags(String message) {
        int firstPosition = message.toUpperCase().indexOf(START_ERR_MSG_TAG);
        int lastPosition = message.toUpperCase().indexOf(END_ERR_MSG_TAG);
        if (firstPosition > -1 && lastPosition > -1) {
            return message.substring(firstPosition + START_ERR_MSG_TAG.length(), lastPosition);
        }
        return null;
    }

    public static StackTraceElement findFirstVqsStackTraceElem(StackTraceElement[] elems) {
        for (StackTraceElement elem : elems) {
            if (elem.getClassName().startsWith("com.onevizion")) {
                return elem;
            }
        }
        return null;
    }

    public static String getSubj(Exception e, String version) {
        StringBuilder subj = new StringBuilder();
        StackTraceElement elemToLog = findFirstVqsStackTraceElem(e.getStackTrace());
        boolean isExternalLibException = false;
        boolean isExceptionInFilter = isFilterClass(e.getStackTrace()[0]);
        if ((elemToLog == null || isFilterClass(elemToLog)) && !isExceptionInFilter) {
            StackTraceElement[] stackTraceElems = e.getStackTrace();
            if (stackTraceElems.length > 0) {
                elemToLog = e.getStackTrace()[0];
                isExternalLibException = true;
            }
        }

        subj.append("[");
        subj.append(version);
        subj.append("] ");
        if (elemToLog == null) {
            subj.append(getUserFriendlyErrMessage(e));
            subj.append(" Stack Trace is not available");
        } else {
            subj.append(elemToLog.getClassName());
            subj.append(".");
            subj.append(elemToLog.getMethodName());
            if (!isExternalLibException) {
                subj.append(" at [");
                subj.append(elemToLog.getLineNumber());
                subj.append("]");
            }
        }
        return subj.toString();
    }

    private static boolean isFilterClass(StackTraceElement e) {
        return e.getClassName().startsWith("com.onevizion.web.filter.");
    }

}
