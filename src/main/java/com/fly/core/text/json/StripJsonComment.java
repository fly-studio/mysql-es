package com.fly.core.text.json;

public class StripJsonComment {

    private enum CommentType {
        None,
        Single,
        Multi
    }

    private StripJsonComment()
    {

    }

    public static String strip(String str)
    {
        boolean insideString = false;
        CommentType insideComment = CommentType.None;
        int offset = 0;
        StringBuilder ret = new StringBuilder();

        char currentChar, nextChar;

        for (int i = 0; i < str.length() - 1; i++)
        {
            currentChar = str.charAt(i);
            nextChar = str.charAt(i + 1);

            if (insideComment == CommentType.None && currentChar == '"') {
                if (i >= 2 && !(str.charAt(i - 1) == '\\' && str.charAt(i - 2) != '\\')) { // not \\"
                    insideString = !insideString;
                }
            }

            if (insideString) {
                continue;
            }

            if (insideComment == CommentType.None && currentChar == '/' && nextChar == '/') { // starts with //
                ret.append(str.substring(offset, i));
                offset = i;
                insideComment = CommentType.Single;
                i++;
            } else if (insideComment == CommentType.Single && currentChar == '\r' && nextChar == '\n') { // end with \r\n
                i++;
                insideComment = CommentType.None;
                offset = i;
            } else if (insideComment == CommentType.Single && (currentChar == '\n' || currentChar == '\r')) { //end with \n (unix) or \r (mac)
                insideComment = CommentType.None;
                offset = i;
            } else if (insideComment == CommentType.None && currentChar == '/' && nextChar == '*') { // starts with /*
                ret.append(str.substring(offset, i));
                offset = i;
                insideComment = CommentType.Multi;
                i++;
            } else if (insideComment == CommentType.Multi && currentChar == '*' && nextChar == '/') {
                i++;
                insideComment = CommentType.None;
                offset = i + 1;
            }
        }

        return ret.append(insideComment == CommentType.None ? str.substring(offset) : "").toString();
    }
}
