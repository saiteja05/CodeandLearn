public class urlConvert {

    public static void main(String[] args) {
      //char[] str = {'M','R',' ','J','O','H','N',' ','S','M','I','T','H','X','X','X','X'};
      char[] str ={' ','X','X'};
        int alength = 1;
     urlMaker(str,alength);
     System.out.println(str);
    }


    public static char[] urlMaker(char[] str, int actuallength)
    {
        if(actuallength==0)
        {
        return str;
        }
        if(actuallength==1)
        {
            if(str[0]==' ')
            {
                str[0]='%';
                str[1]='2';
                str[2]='0';
            }

            return str;
        }
        int spacecount=0;

        for (int i=0;i<actuallength;i++)
        {
            if(str[i]==' ')
            {
                ++spacecount;
            }
        }
        int newlength=(actuallength+spacecount*2)-1;
        //System.out.println(newlength);
        for (int i=actuallength-1;i>=0;--i)
        {       // System.out.println(i);
            //System.out.println(newlength);


            if(str[i]==' ')
            {
                str[newlength]='0';
                str[newlength-1]='2';
                str[newlength-2]='%';

                newlength=newlength-3;
            }
            else
            {
                str[newlength] = str[i];
                newlength = newlength - 1;
            }
//System.out.println(str[i]);
        }

        return str;

    }


//        public static void main(String[] args) {
//            String input = "This  is a   sample   string";
//            String replacement = "%20";
//            String replacedString = replaceStringLiteral(input, replacement);
//            System.out.println(replacedString);
//        }
//
//        public static String replaceStringLiteral(String input, String replacement) {
//            StringBuilder stringBuilder = new StringBuilder();
//            int length = input.length();
//            boolean isSpace = false;
//
//            for (int i = 0; i < length; i++) {
//                char currentChar = input.charAt(i);
//
//                if (currentChar == ' ' && !isSpace) {
//                    // Replace the string literal with the desired replacement
//                    stringBuilder.append(replacement);
//                    isSpace = true;
//                } else if (currentChar != ' ') {
//                    stringBuilder.append(currentChar);
//                    isSpace = false;
//                }
//            }
//
//            return stringBuilder.toString();
//        }
    }


