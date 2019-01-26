package ceng.ceng351.musicdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

public class MUSICDB {

    private static String user = "e2167617";//"pa1_user";
    private static String password = "bc4d1622"; //"123456";
    private static String host = "144.122.71.57"; //"localhost";
    private static String database = "db2167617";//"pa1_recitation";
    private static int port = 8084; // 3306;

    private Connection con;

    public void initialize(){

        String url = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.con =  DriverManager.getConnection(url, this.user, this.password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private int implementQueries(String queries[]){
        int result;
        int count = 0;
        for(String query : queries) {
            try {
                Statement statement = this.con.createStatement();

                result = statement.executeUpdate(query);
                System.out.println(result);
                count++;

                //close
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    public int createTables(){

        String[] queriesCreateTable = new String[5];

        //user(userID:int, userName:varchar(60), email:varchar(30), password:varchar(30))
        queriesCreateTable[0] = "create table user (" +
                "userID int not null," +
                "userName varchar(60)," +
                "email varchar(30)," +
                "password varchar(30)," +
                "primary key (userID));";

        //artist(artistID:int, artistName:varchar(60))
        queriesCreateTable[1] = "create table artist (" +
                "artistID int not null," +
                "artistName varchar(60)," +
                "primary key (artistID));";

        //album(albumID:int, title:varchar(60), albumGenre:varchar(30), albumRating:double, releaseDate:date, artistID:int)
        queriesCreateTable[2] = "create table album (" +
                "albumID int not null," +
                "title varchar(60)," +
                "albumGenre varchar(30)," +
                "albumRating double," +
                "releaseDate date," +
                "artistID int," +
                "primary key (albumID)," +
                "foreign key (artistID) References artist(artistID));";

        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        queriesCreateTable[3] = "create table song (" +
                "songID int not null," +
                "songName varchar(60)," +
                "genre varchar(30)," +
                "rating double," +
                "artistID int," +
                "albumID int," +
                "primary key (songID)," +
                "foreign key (artistID) References artist(artistID)," +
                "foreign key (albumID) References album(albumID));";

        //listen(userID:int, songID:int, lastListenTime:timestamp, listenCount:int)
        queriesCreateTable[4] = "create table listen (" +
                "userID int not null," +
                "songID int not null," +
                "lastListenTİme timestamp," +
                "listenCount int," +
                "primary key (userID, songID)," +
                "foreign key (songID) References song(songID) On Delete Cascade On Update Cascade );";

        return implementQueries(queriesCreateTable);

    }

    public int dropTables(){

        String[] queriesDropTable = new String[5];

        queriesDropTable[0] = "drop table if exists listen";
        queriesDropTable[1] = "drop table if exists song";
        queriesDropTable[2] = "drop table if exists album";
        queriesDropTable[3] = "drop table if exists user";
        queriesDropTable[4] = "drop table if exists artist";

        return implementQueries(queriesDropTable);

    }

    public int insertAlbum(Album[] albums) {

        String[] queries = new String[albums.length];

        for (int i=0; i<albums.length; i++) {
            queries[i] = "insert into album values ('" +
                    albums[i].getAlbumID() + "','" +
                    albums[i].getTitle().replace("'", "/") + "','" +
                    albums[i].getAlbumGenre().replace("'", "/")  + "','" +
                    albums[i].getAlbumRating() + "','" +
                    albums[i].getReleaseDate() + "','" +
                    albums[i].getArtistID() + "')";
        }

        return implementQueries(queries);

    }

    public int insertArtist(Artist[] artists) {

        String[] queries = new String[artists.length];

        for (int i = 0; i < artists.length; i++) {
            queries[i] = "insert into artist values ('" +
                    artists[i].getArtistID() + "','" +
                    artists[i].getArtistName().replace("'", "/")  + "')";

        }
        return implementQueries(queries);
    }

    public int insertSong(Song[] songs){

        String[] queries = new String[songs.length];

        for (int i = 0; i < songs.length; i++) {
            queries[i] = "insert into song values ('" +
                    songs[i].getSongID() + "','" +
                    songs[i].getSongName().replace("'", "/")  + "','" +
                    songs[i].getGenre().replace("'", "/")  + "','" +
                    songs[i].getRating() + "','" +
                    songs[i].getArtistID() + "','" +
                    songs[i].getAlbumID() + "')";
        }

        return implementQueries(queries);

    }

    public int insertUser(User[] users){

        String[] queries = new String[users.length];

        for (int i = 0; i < users.length; i++) {
            queries[i] = "insert into user values ('" +
                    users[i].getUserID() + "','" +
                    users[i].getUserName().replace("'", "/")  + "','" +
                    users[i].getEmail().replace("'", "/")  + "','" +
                    users[i].getPassword().replace("'", "/")  + "')";

        }

        return implementQueries(queries);

    }

    public int insertListen(Listen[] listens){

        String[] queries = new String[listens.length];

            for (int i = 0; i < listens.length; i++) {
            queries[i] = "insert into listen values ('" +
                    listens[i].getUserID() + "','" +
                    listens[i].getSongID() + "','" +
                    listens[i].getLastListenTime() + "','" +
                    listens[i].getListenCount() + "')";
        }

        return implementQueries(queries);

    }

    public QueryResult.ArtistNameSongNameGenreRatingResult[] getHighestRatedSongs(){

        Vector<QueryResult.ArtistNameSongNameGenreRatingResult> vectorResult = new Vector(0);
        QueryResult.ArtistNameSongNameGenreRatingResult[] result;
        ResultSet queryResult;


        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        String query = "Select A.artistName, S.songName, S.genre, S.rating " +
                        "From song S, artist A " +
                        "Where A.artistID=S.artistID AND S.rating >= (Select MAX(S2.rating) From song S2);";

        //First call method next() to make the first row current row.
        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);

            while(queryResult.next()) {
                String artistName = queryResult.getString("artistName");
                String songName = queryResult.getString("songName");
                String genre = queryResult.getString("genre");
                double rating = queryResult.getDouble("rating");
                vectorResult.addElement(new QueryResult.ArtistNameSongNameGenreRatingResult(artistName, songName, genre, rating));
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        result = vectorResult.toArray(new QueryResult.ArtistNameSongNameGenreRatingResult[vectorResult.size()]);

        return result;
    }

    public QueryResult.TitleReleaseDateRatingResult getMostRecentAlbum(String artistName){

        QueryResult.TitleReleaseDateRatingResult result = null;
        ResultSet queryResult;

        //artist(artistID:int, artistName:varchar(60))
        //album(albumID:int, title:varchar(60), albumGenre:varchar(30), albumRating:double, releaseDate:date, artistID:int)
        String query = "Select AL.title, AL.releaseDate, AL.albumRating " +
                "From album AL, artist AR " +
                "Where AL.artistID=AR.artistID AND AR.artistName='" + artistName.trim() + "' AND AL.releaseDate >= (Select MAX(AL2.releaseDate) From album AL2 Where AL2.artistID=AR.artistID);";

        //First call method next() to make the first row current row.
        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);
            queryResult.next();
            String title = queryResult.getString("title");
            String releaseDate = queryResult.getString("releaseDate");
            double albumRating = queryResult.getDouble("albumRating");
            result = new QueryResult.TitleReleaseDateRatingResult(title, releaseDate, albumRating);
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    public QueryResult.ArtistNameSongNameGenreRatingResult[] getCommonSongs(String userName1, String userName2){

        Vector<QueryResult.ArtistNameSongNameGenreRatingResult> vectorResult = new Vector(0);
        QueryResult.ArtistNameSongNameGenreRatingResult[] result;
        ResultSet queryResult;

        //listen(userID:int, songID:int, lastListenTime:timestamp, listenCount:int)
        //user(userID:int, userName:varchar(60), email:varchar(30), password:varchar(30))
        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        String query = "Select A.artistName, S.songName, S.genre, S.rating " +
                    "From song S, listen L1, listen L2, user U1, user U2, artist A " +
                    "Where A.artistID=S.artistID AND L1.userID=U1.userID AND U1.userName='" + userName1.trim() + "' AND L2.userID=U2.userID AND U2.userName='" + userName2.trim() + "' AND L1.songID=L2.songID AND L1.songID=S.songID;" ;

        //First call method next() to make the first row current row.
        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);
            while(queryResult.next()) {
                String artistName = queryResult.getString("artistName");
                String songName = queryResult.getString("songName");
                String genre = queryResult.getString("genre");
                double rating = queryResult.getDouble("rating");
                vectorResult.addElement(new QueryResult.ArtistNameSongNameGenreRatingResult(artistName, songName, genre, rating));
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        result = vectorResult.toArray(new QueryResult.ArtistNameSongNameGenreRatingResult[vectorResult.size()]);

        return result;
    }

    public QueryResult.ArtistNameNumberOfSongsResult[] getNumberOfTimesSongsListenedByUser(String userName){

        Vector<QueryResult.ArtistNameNumberOfSongsResult> vectorResult = new Vector(0);
        QueryResult.ArtistNameNumberOfSongsResult[] result;
        ResultSet queryResult;

        //artist(artistID:int, artistName:varchar(60))
        //listen(userID:int, songID:int, lastListenTime:timestamp, listenCount:int)
        //user(userID:int, userName:varchar(60), email:varchar(30), password:varchar(30))
        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        String query = "Select A.artistName, Sum(L.listenCount) as count " +
                "From song S, listen L, artist A, user U " +
                "Where U.userName = '" + userName.trim() + "' AND U.userID=L.userID AND L.songID=S.songID AND S.artistID=A.artistID " +
                "Group By A.artistID; ";

        //First call method next() to make the first row current row.
        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);
            while(queryResult.next()) {
                String artistName = queryResult.getString("artistName");
                int count = queryResult.getInt("count");
                vectorResult.addElement(new QueryResult.ArtistNameNumberOfSongsResult(artistName, count));
            }
            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        result = vectorResult.toArray(new QueryResult.ArtistNameNumberOfSongsResult[vectorResult.size()]);

        return result;
    }

    public User[] getUsersWhoListenedAllSongs(String artistName){

        Vector<User> vectorResult = new Vector(0);
        User[] result;
        ResultSet queryResult;

        //artist(artistID:int, artistName:varchar(60))
        //listen(userID:int, songID:int, lastListenTime:timestamp, listenCount:int)
        //user(userID:int, userName:varchar(60), email:varchar(30), password:varchar(30))
        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        String query = "Select U.userID, U.userName, U.email, U.password " +
                        "From user U, artist A " +
                        "Where A.artistName = '" + artistName.trim() + "' AND Not Exists " +
                            "(Select * "+
                            "From song S1 " +
                            "Where S1.artistID=A.artistID AND S1.songID Not In " +
                            "(Select S2.songID " +
                            "From song S2, listen L " +
                            "Where S2.songID=L.songID AND L.userID=U.userID)); ";

        //First call method next() to make the first row current row.
        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);
            while(queryResult.next()) {
                int userID = queryResult.getInt("userID");
                String userName = queryResult.getString("userName");
                String email = queryResult.getString("email");
                String password = queryResult.getString("password");
                vectorResult.addElement(new User(userID, userName, email, password));
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        result = vectorResult.toArray(new User[vectorResult.size()]);

        return result;
    }

    public QueryResult.UserIdUserNameNumberOfSongsResult[]  getUserIDUserNameNumberOfSongsNotListenedByAnyone(){

        Vector<QueryResult.UserIdUserNameNumberOfSongsResult> vectorResult = new Vector(0);
        QueryResult.UserIdUserNameNumberOfSongsResult[] result;
        ResultSet queryResult;

        //artist(artistID:int, artistName:varchar(60))
        //listen(userID:int, songID:int, lastListenTime:timestamp, listenCount:int)
        //user(userID:int, userName:varchar(60), email:varchar(30), password:varchar(30))
        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        String query = "Select U.userID, U.userName, COUNT(*) as count " +
                        "From user U, song S2, listen L2 " +
                        "Where S2.songID=L2.songID AND L2.userID=U.userID AND S2.songID<>ALL " +
                            "(Select S1.songID " +
                            "From song S1, listen L1 " +
                            "Where S1.songID=L1.songID AND L1.userID<>U.userID) " +
                        "Group By U.userID; ";

        //First call method next() to make the first row current row.
        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);
            while(queryResult.next()) {
                int userID = queryResult.getInt("userID");
                String userName = queryResult.getString("userName");
                int count = queryResult.getInt("count");
                vectorResult.addElement(new QueryResult.UserIdUserNameNumberOfSongsResult(userID, userName, count));
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        result = vectorResult.toArray(new QueryResult.UserIdUserNameNumberOfSongsResult[vectorResult.size()]);

        return result;
    }

    public Artist[] getArtistSingingPopGreaterAverageRating(double rating){

        Vector<Artist> vectorResult = new Vector(0);
        Artist[] result;
        ResultSet queryResult;

        //artist(artistID:int, artistName:varchar(60))
        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        String query = "Select A.artistID, A.artistName " +
                "From artist A " +
                "Where (Select AVG(S1.rating) From song S1 Where  A.artistID=S1.artistID) > " + rating + " AND Exists" +
                    "(Select * "+
                    "From song S2 " +
                    "Where S2.artistID=A.artistID AND S2.genre='pop'); ";

        //First call method next() to make the first row current row.
        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);
            while(queryResult.next()) {
                int artistID = queryResult.getInt("artistID");
                String artistName = queryResult.getString("artistName");
                vectorResult.addElement(new Artist(artistID, artistName));
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        result = vectorResult.toArray(new Artist[vectorResult.size()]);

        return result;
    }

    public Song[] retrieveLowestRatedAndLeastNumberOfListenedSongs(){

        Vector<Song> vectorResult = new Vector(0);
        Song[] result;
        ResultSet queryResult;

        //listen(userID:int, songID:int, lastListenTime:timestamp, listenCount:int)
        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        String query = "Select distinct Temp.songID, Temp.songName, Temp.genre, Temp.rating, Temp.artistID, Temp.albumId " +
                        "From  (Select S.songID, S.songName, S.genre, S.rating, S.artistID, S.albumId, Sum(L.listenCount) as count " +
                                "From song S, listen L " +
                                "Where S.genre ='pop' AND S.rating <= (Select MIN(S2.rating) From song S2 Where S2.genre='pop') AND S.songID=L.songID " +
                                "Group By S.songId) as Temp " +
                        "Where Temp.count <= (Select Min(Temp2.count2) " +
                                            "From (Select Sum(L2.listenCount) as count2 " +
                                                    "From song S3, listen L2 " +
                                                    "Where S3.genre ='pop' AND S3.rating <= (Select MIN(S4.rating) From song S4 Where S4.genre='pop') AND S3.songID=L2.songID " +
                                                    "Group By S3.songID) as Temp2);";

        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);
            while(queryResult.next()) {
                int songID = queryResult.getInt("songID");
                String songName = queryResult.getString("songName");
                String genre = queryResult.getString("genre");
                double rating = queryResult.getDouble("rating");
                int artistID = queryResult.getInt("artistID");
                int albumID = queryResult.getInt("albumID");
                vectorResult.addElement(new Song(songID, songName, genre, rating, artistID, albumID));
            }
            st.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }

        result = vectorResult.toArray(new Song[vectorResult.size()]);

        return result;
    }

    public int multiplyRatingOfAlbum(String releaseDate){

        int rowCount = 0;

        //album(albumID:int, title:varchar(60), albumGenre:varchar(30), albumRating:double, releaseDate:date, artistID:int)
        String query = "Update album A " +
                "Set A.albumRating = A.albumRating * 1.5 " +
                "Where A.releaseDate > " + Integer.parseInt(releaseDate.trim().replaceAll("-", "")) + " ;";

        try {
            Statement st = this.con.createStatement();
            rowCount = st.executeUpdate(query);
            st.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }

        return rowCount;
    }

    public Song deleteSong(String songName){

        ResultSet queryResult;
        Song result = null;

        String query = "Select S.songID, S.songName, S.genre, S.rating, S.artistID, S.albumID From song S Where S.songName ='" + songName.trim() + "' ;";

        try {
            Statement st = this.con.createStatement();
            queryResult = st.executeQuery(query);
            queryResult.next();
            int songID = queryResult.getInt("songID");
            songName = queryResult.getString("songName");
            String genre = queryResult.getString("genre");
            double rating = queryResult.getDouble("rating");
            int artistID = queryResult.getInt("artistID");
            int albumID = queryResult.getInt("albumID");
            result = new Song(songID, songName, genre, rating, artistID, albumID);
            st.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }

        //song(songID:int, songName:varchar(60), genre:varchar(30), rating:double, artistID:int, albumID:int)
        query = "Delete From song " +
                "Where songName= '" + songName.trim() + "' ;";

        try {
            Statement st = this.con.createStatement();
            st.executeUpdate(query);
            st.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }


        return result;
    }

}
