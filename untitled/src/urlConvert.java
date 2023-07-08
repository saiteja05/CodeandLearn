public class urlConvert {
//
//    public static void main(String[] args) {
//      char[] str = {'M','R',' ','J','O','H','N',' ','S','M','I','T','H','','','',''};
//      int alength = 13;
//    }
//
//
//    public static String urlMaker(Char[] str, int actuallength)
//    {
//        char[] =
//    }


        public static void main(String[] args) {
            String input = "This  is a   sample   string";
            String replacement = "%20";
            String replacedString = replaceStringLiteral(input, replacement);
            System.out.println(replacedString);
        }

        public static String replaceStringLiteral(String input, String replacement) {
            StringBuilder stringBuilder = new StringBuilder();
            int length = input.length();
            boolean isSpace = false;

            for (int i = 0; i < length; i++) {
                char currentChar = input.charAt(i);

                if (currentChar == ' ' && !isSpace) {
                    // Replace the string literal with the desired replacement
                    stringBuilder.append(replacement);
                    isSpace = true;
                } else if (currentChar != ' ') {
                    stringBuilder.append(currentChar);
                    isSpace = false;
                }
            }

            return stringBuilder.toString();
        }
    }


