package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly))
        {

            FirstNameInfo info = new FirstNameInfo();

            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Num, First_Name, LENGTH(First_Name) AS length " +
                "FROM " + UsersTable + " " +
                "GROUP BY First_Name " +
                "ORDER BY length DESC, First_Name ASC");

            boolean found_longest = false;
            boolean found_shortest = false;

           FakebookArrayList<String> temp = new FakebookArrayList<String>(", ");


           rst.next();

           int prev_x = rst.getInt("length");
           String prev_y = rst.getString("First_Name");


           if (rst.isFirst())
                 {
                    temp.add(rst.getString("First_Name"));
                    for(int i = 0; i < temp.size(); i++)
                    {
                        info.addLongName(temp.get(i));
                    }
                    temp.clear();
                }
                if (rst.isLast())
                {
                    temp.add(rst.getString("First_Name"));
                    for(int i = 0; i < temp.size(); i++)
                    {
                        info.addShortName(temp.get(i));
                    }
                    temp.clear();
                }



            while (rst.next()) {


                if (prev_x == rst.getInt("length"))
                {
                    temp.add(rst.getString("First_Name"));
                }
                else
                {
                    temp.clear();
                    prev_x = rst.getInt("length");
                    temp.add(rst.getString("First_Name"));
                }

                if (rst.isFirst())
                 {
                    //temp.add(rst.getString("First_Name"));
                    for(int i = 0; i < temp.size(); i++)
                    {
                        info.addLongName(temp.get(i));
                    }
                    temp.clear();
                }
                if (rst.isLast())
                {
                    //temp.add(rst.getString("First_Name"));
                    for(int i = 0; i < temp.size(); i++)
                    {
                        info.addShortName(temp.get(i));
                    }
                    temp.clear();
                }
                prev_x = rst.getInt("length");
                prev_y = rst.getString("First_Name");
            }


            ResultSet rst1 = stmt.executeQuery(
                "SELECT * " +
                "FROM " + UsersTable + " " +
                "ORDER BY First_Name ASC");

            rst1.next();

            String prev = rst1.getString("First_Name");
            int running_total = 1;
            String current_name = prev;
            int total = 1;
            String final_name = prev;

            while(rst1.next())
            {
                if(prev.equals(rst1.getString("First_Name")))
                {
                    running_total++;
                }
                else
                {
                    if(total < running_total)
                    {
                        final_name = current_name;
                        total = running_total;
                    }
                    running_total = 1;
                    current_name = rst1.getString("First_Name");
                }

                prev = rst1.getString("First_Name");
            }

            info.addCommonName(final_name);
            info.setCommonNameCount(total);

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */

            rst.close();
            stmt.close();
            return info;                // placeholder for compilation
        }
        catch (SQLException e)
        {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly))
        {
          ResultSet rst = stmt.executeQuery(
          "SELECT allUsers.USER_ID, allUsers.FIRST_NAME, allUsers.LAST_NAME " +
          "FROM " + UsersTable + " allUsers " +
          "WHERE NOT EXISTS ( " +
          "SELECT allFriends.USER1_ID, allFriends.USER2_ID " +
          "FROM " + FriendsTable + " allFriends " +
          "WHERE allUsers.USER_ID = allFriends.USER1_ID OR allUsers.User_ID = allFriends.USER2_ID ) ");

          while(rst.next()) {
            UserInfo newUser = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
            results.add(newUser);
          }
          rst.close();
          stmt.close();

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

          ResultSet rst = stmt.executeQuery(
          "SELECT allUsers.USER_ID, allUsers.FIRST_NAME, allUsers.LAST_NAME " +
          "FROM " + UsersTable + " allUsers, " + CurrentCitiesTable + " allCities, " + HometownCitiesTable + " allHometowns " +
          "WHERE " +
          "allUsers.USER_ID = allCities.USER_ID AND " +
          "allUsers.USER_ID = allHometowns.USER_ID AND " +
          "allCities.CURRENT_CITY_ID <> allHometowns.HOMETOWN_CITY_ID " +
          "ORDER BY allUsers.USER_ID ASC");


          while(rst.next()) {
            UserInfo newUser = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
            results.add(newUser);
          }
          rst.close();
          stmt.close();






            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
             Statement innerStmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)){

          ResultSet rst = stmt.executeQuery(
          "SELECT allPhotos.PHOTO_ID, allAlbums.ALBUM_ID, allPhotos.PHOTO_LINK, allAlbums.ALBUM_NAME, COUNT(allTags.TAG_PHOTO_ID) " +
          "FROM " + AlbumsTable + " allAlbums, " + PhotosTable + " allPhotos, " + TagsTable + " allTags " +
          "WHERE " +
          "allAlbums.ALBUM_ID = allPhotos.ALBUM_ID AND "  +
          "allPhotos.PHOTO_ID = allTags.TAG_PHOTO_ID " +
          "GROUP BY allPhotos.PHOTO_ID, allAlbums.ALBUM_ID, allPhotos.PHOTO_LINK, allAlbums.ALBUM_NAME, allTags.TAG_PHOTO_ID " +
          "ORDER BY COUNT(allTags.TAG_PHOTO_ID) DESC, allPhotos.PHOTO_ID ASC ");

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */

            int numberProcessed = 0;
            FakebookArrayList<TaggedPhotoInfo> temp = new FakebookArrayList<TaggedPhotoInfo>("\n");
            long currentPhoto = 0L;

            while (rst.next() && numberProcessed < num ){
              PhotoInfo newPhoto = new PhotoInfo(rst.getLong(1), rst.getLong(2), rst.getString(3), rst.getString(4));
              TaggedPhotoInfo newTag = new TaggedPhotoInfo(newPhoto);

              currentPhoto = rst.getLong(1);

              ResultSet names = innerStmt.executeQuery(
              "SELECT allUsers.USER_ID, allUsers.FIRST_NAME, allUsers.LAST_NAME " +
              "FROM " + UsersTable + " allUsers, " + TagsTable + " allTags, " + PhotosTable + " allPhotos " +
              "WHERE " +
              "allUsers.USER_ID = allTags.TAG_SUBJECT_ID AND " +
              "allTags.TAG_PHOTO_ID = allPhotos.PHOTO_ID AND " +
              "allTags.TAG_PHOTO_ID = " + currentPhoto + " " +
              "ORDER BY allUsers.USER_ID ASC");

              while (names.next()){
                newTag.addTaggedUser(new UserInfo(names.getLong(1), names.getString(2), names.getString(3)));
              }

              results.add(newTag);
              numberProcessed++;
            }

            rst.close();
            stmt.close();
            innerStmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }



        return results;
    }

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

          ResultSet rst = stmt.executeQuery(
          "SELECT DISTINCT COUNT(*), allCities.STATE_NAME " +
          "FROM " + CitiesTable  + " allCities, " + EventsTable + " allEvents "  +
          "WHERE " +
          "allCities.CITY_ID = allEvents.EVENT_CITY_ID " +
          "GROUP BY allCities.STATE_NAME " +
          "ORDER BY COUNT(*) DESC, allCities.STATE_NAME ASC");

          rst.next();
          int maximumEvents = rst.getInt(1);
          EventStateInfo popularStates = new EventStateInfo(rst.getInt(1));
          popularStates.addState(rst.getString(2));

          while(rst.next()){
            if (maximumEvents == rst.getInt(1)){
              popularStates.addState(rst.getString(2).toString());
            }
          }
          rst.close();
          stmt.close();
          return popularStates;
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {

          ResultSet oldest = stmt.executeQuery(
          "SELECT DISTINCT allUsers.USER_ID, allUsers.FIRST_NAME, allUsers.LAST_NAME, allUsers.MONTH_OF_BIRTH, allUsers.DAY_OF_BIRTH, allUsers.YEAR_OF_BIRTH " +
          "FROM " + UsersTable + " allUsers, " + FriendsTable + " allFriends " +
          "WHERE " +
          "(allFriends.USER1_ID =" + userID + " OR allFriends.USER2_ID =" + userID + " ) AND "  +
          "(allUsers.USER_ID = allFriends.USER1_ID OR allUsers.USER_ID = allFriends.USER2_ID) " +
          "ORDER BY allUsers.YEAR_OF_BIRTH ASC, allUsers.DAY_OF_BIRTH ASC, allUsers.MONTH_OF_BIRTH ASC");

          oldest.next();
          UserInfo oldestUser = new UserInfo(oldest.getLong(1), oldest.getString(2), oldest.getString(3));


          oldest.close();


          ResultSet youngest = stmt.executeQuery(
          "SELECT DISTINCT allUsers.USER_ID, allUsers.FIRST_NAME, allUsers.LAST_NAME, allUsers.MONTH_OF_BIRTH, allUsers.DAY_OF_BIRTH, allUsers.YEAR_OF_BIRTH " +
          "FROM " + UsersTable + " allUsers, " + FriendsTable + " allFriends " +
          "WHERE " +
          "(allFriends.USER1_ID =" + userID + " OR allFriends.USER2_ID =" + userID + " ) AND "  +
          "(allUsers.USER_ID = allFriends.USER1_ID OR allUsers.USER_ID = allFriends.USER2_ID) " +
          "ORDER BY allUsers.YEAR_OF_BIRTH DESC, allUsers.DAY_OF_BIRTH DESC, allUsers.MONTH_OF_BIRTH DESC");


          youngest.next();
          UserInfo youngestUser = new UserInfo (youngest.getLong(1), youngest.getString(2), youngest.getString(3));

          youngest.close();
          stmt.close();

          return new AgeInfo(oldestUser, youngestUser);

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
