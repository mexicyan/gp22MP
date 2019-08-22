/**
 * Copyright (C), 2015-2019, XXX有限公司
 * FileName: ConnectionPool
 * Author: 12192
 * Date: 2019/8/22 15:40
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * 〈连接池连接上传数据〉
 * 〈〉
 *
 * @author 12192
 * @create 2019/8/22
 * @since 1.0.0
 */
public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0; i < 5; i++) {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://haodoop02:3306/exam?useUnicode=true&characterEncoding=utf8",
                            "root",
                            "root");
                    connectionQueue.push(conn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();

    }

    public  static void returnConnection(Connection conn){connectionQueue.push(conn);}
}
