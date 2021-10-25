import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;

public class Select implements Serializable {

    public static void main(String[] args){

        String urlConexion = "jdbc:sqlserver://localhost;databaseName=Bankline;integratedSecurity=true";
        int filas;
        try(Connection cnx = DriverManager.getConnection(urlConexion)){
            /*/
            Statement stmt = cnx.createStatement();
            if(! cnx.isClosed()){
                // Sentencia que devuelve un único valor
                String sentencia = "SELECT COUNT(*) AS total FROM Movimiento WHERE numero='3310001515'";
                // Creo una variable de tipo ResultSet para recibir los valores de la sentencia SELECT
                ResultSet rs = stmt.executeQuery(sentencia);
                // Posiciono el puntero en el siguiente registro o el primero
                rs.next();
                // Si se el tipo de valor que viene
                int cantidad = rs.getInt("total");
                System.out.println(cantidad);

                // Sentencia que devuelve varios valores
                sentencia = "SELECT * FROM Movimiento WHERE numero='3310001515'";
                rs = stmt.executeQuery(sentencia);
                while(rs.next()){
                    System.out.println(rs.getInt("idMovimiento") + ". . . ." + rs.getBigDecimal("importe"));
                }

                // Sentencia con parámetros
                sentencia = "SELECT * FROM Movimiento WHERE numero=?";
                PreparedStatement pstmt = cnx.prepareStatement(sentencia);
                pstmt.setString(1,"3310001515");
                rs = pstmt.executeQuery();
                System.out.println("Con parámetros");
                while(rs.next()){
                    System.out.println(rs.getInt("idMovimiento") + ". . . ." + rs.getBigDecimal("importe"));
                }
                */
            // Simple sin parametros
            Object objeto;
            String sentencia = "SELECT COUNT(*) AS total FROM Movimiento WHERE numero='3310001515'";
            objeto = consultarEscalar(cnx, sentencia);
            System.out.println("Simple sin parámetros");
            System.out.println(objeto);
            // Simple con parámetros
            sentencia = "SELECT COUNT(*) AS total FROM Movimiento WHERE numero=?";
            ArrayList<Object> parametros = new ArrayList<>();
            parametros.add("3310001515");
            objeto = consultarEscalar(cnx, sentencia, parametros);
            System.out.println("Simple con parámetros");
            System.out.println(objeto);
            // ResultSet simple
            sentencia = "SELECT * FROM Movimiento WHERE numero='3310001515'";
            ResultSet rs;
            rs = consultarMultiple(cnx, sentencia);
            System.out.println("ResultSet simple");
            while(rs.next()){
                System.out.println("Id: " + rs.getObject(1) + " Número: " + rs.getObject(2) + " Fecha: " +
                        rs.getObject(3) + " Valor: " + rs.getObject(4) + " Concepto: " +
                        rs.getObject(5) + " Importe: " + rs.getObject(6));
            }
            // ResultSet con parámetros
            sentencia = "SELECT * FROM Movimiento WHERE numero=?";
            rs = consultarMultiple(cnx, sentencia, parametros);
            System.out.println("ResultSet con parámetros");
            while(rs.next()){
                System.out.println("Id: " + rs.getObject(1) + " Número: " + rs.getObject(2) + " Fecha: " +
                        rs.getObject(3) + " Valor: " + rs.getObject(4) + " Concepto: " +
                        rs.getObject(5) + " Importe: " + rs.getObject(6));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Object consultarEscalar(Connection cnx, String sentencia){
        Object resultado = null;
        try{
            Statement stmt = cnx.createStatement();
            if(! cnx.isClosed()){
                ResultSet rs = stmt.executeQuery(sentencia);
                rs.next();
                resultado =  rs.getObject(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return resultado;
    }

    public static Object consultarEscalar(Connection cnx, String sentencia, ArrayList<Object> args){
        ResultSet rs = null;
        Object resultado = null;
        try{
            if(! cnx.isClosed()){
                PreparedStatement pstmt = cnx.prepareStatement(sentencia);
                ParameterMetaData pmd = pstmt.getParameterMetaData();
                if(! cnx.isClosed()){
                    if(pmd.getParameterCount() == args.size()){
                        for(int i = 0; i < args.size(); i++)
                            pstmt.setObject((i + 1), args.get(i));
                        rs = pstmt.executeQuery();
                        rs.next();
                        resultado = rs.getObject(1);
                    }
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return resultado;
    }

    public static ResultSet consultarMultiple(Connection cnx, String sentencia){
        ResultSet rs = null;
        try{
            Statement stmt = cnx.createStatement();
            if(! cnx.isClosed()){
                rs = stmt.executeQuery(sentencia);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return rs;
    }

    public static ResultSet consultarMultiple(Connection cnx, String sentencia, ArrayList<Object> args){
        ResultSet rs = null;
        try{
            PreparedStatement pstmt = cnx.prepareStatement(sentencia);
            ParameterMetaData pmd = pstmt.getParameterMetaData();
            if(! cnx.isClosed()){
                if(pmd.getParameterCount() == args.size()){
                    for(int i = 0; i < args.size(); i++)
                        pstmt.setObject((i + 1), args.get(i));
                    rs = pstmt.executeQuery();
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return rs;
    }
}
