package appserver.satellite;

import appserver.job.Job;
import appserver.comm.ConnectivityInfo;
import appserver.job.UnknownToolException;
import appserver.comm.Message;
import static appserver.comm.MessageTypes.JOB_REQUEST;
import static appserver.comm.MessageTypes.REGISTER_SATELLITE;
import appserver.job.Tool;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.server.Operation;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import utils.PropertyHandler;
import java.util.Properties;

/**
 * Class [Satellite] Instances of this class represent computing nodes that execute jobs by
 * calling the callback method of tool a implementation, loading the tool's code dynamically over a network
 * or locally from the cache, if a tool got executed before.
 *
 * @author Dr.-Ing. Wolf-Dieter Otte
 */
public class Satellite extends Thread {

    private ConnectivityInfo satelliteInfo = new ConnectivityInfo();
    private ConnectivityInfo serverInfo = new ConnectivityInfo();
    private HTTPClassLoader classLoader = null;
    private Hashtable<String, Tool> toolsCache = null;

    public Satellite(String satellitePropertiesFile, String classLoaderPropertiesFile, String serverPropertiesFile) {

        // read this satellite's properties and populate satelliteInfo object,
        // which later on will be sent to the server
        Properties satelliteProperties = null;
        Properties serverProperties = null;
        Properties classLoaderProperties = null;

        try 
        {
            satelliteProperties = new PropertyHandler(satellitePropertiesFile);
        } catch (IOException e) 
        {
            System.out.println("[Sattelite] Didn't find properties file \"" + satellitePropertiesFile + "\"");
            System.exit(1);
        }
        
        satelliteInfo.setName(satelliteProperties.getProperty("NAME"));
        satelliteInfo.setPort(Integer.parseInt(satelliteProperties.getProperty("PORT")));
        
        // read properties of the application server and populate serverInfo object
        // other than satellites, the as doesn't have a human-readable name, so leave it out
        try 
        {
            serverProperties = new PropertyHandler(serverPropertiesFile);
        } catch (IOException e) 
        {
            System.out.println("[Sattelite] Didn't find properties file \"" + serverPropertiesFile + "\"");
            System.exit(1);
        }

        serverInfo.setHost(serverProperties.getProperty("HOST"));
        serverInfo.setPort(Integer.parseInt(serverProperties.getProperty("PORT")));
        
        // read properties of the code server and create class loader
        // -------------------
        try 
        {
            classLoaderProperties = new PropertyHandler(classLoaderPropertiesFile);
        } catch (IOException e) 
        {
            System.out.println("[Sattelite] Didn't find properties file \"" + classLoaderPropertiesFile + "\"");
            System.exit(1);
        }
        
        classLoader = new HTTPClassLoader(serverProperties.getProperty("HOST"), 
                                          Integer.parseInt(serverProperties.getProperty("PORT")));

        // create tools cache
        // -------------------
        toolsCache = new Hashtable<String, Tool>();
    }

    @Override
    public void run() {

        // register this satellite with the SatelliteManager on the server
        // ---------------------------------------------------------------
        
        // create server socket
        // ---------------------------------------------------------------
        ServerSocket serverSocket = null;

        try
        {
            serverSocket = new ServerSocket(satelliteInfo.getPort());
        }
        catch (IOException e)
        {
            System.out.println("Error opening server socket");
            System.exit(1);
        }
        
        // start taking job requests in a server loop
        // ---------------------------------------------------------------
        // ...
        while (true) {
            // Accept incoming job requests
            try
            {
                Socket jobRequest = serverSocket.accept();

                // Create a new thread to handle the job request
                new SatelliteThread(jobRequest, this).start();
            }
            catch (IOException e)
            {
                System.out.println("Error accepting request");
            }
        }
    }

    // inner helper class that is instanciated in above server loop and processes single job requests
    private class SatelliteThread extends Thread {

        Satellite satellite = null;
        Socket jobRequest = null;
        ObjectInputStream readFromNet = null;
        ObjectOutputStream writeToNet = null;
        Message message = null;

        SatelliteThread(Socket jobRequest, Satellite satellite) {
            this.jobRequest = jobRequest;
            this.satellite = satellite;
        }

        @Override
        public void run() {
            // setting up object streams
            // ...
            try
            {
                readFromNet = new ObjectInputStream(jobRequest.getInputStream());
                writeToNet = new ObjectOutputStream(jobRequest.getOutputStream());
            }
            catch (IOException e)
            {
                System.out.println("Error getting input/output streams");
            }

            // reading message
            // ...
            try
            {
                message = (Message) readFromNet.readObject();
            }
            catch (IOException | ClassNotFoundException e)
            {
                System.out.println("Error reading from stream");
            }

            switch (message.getType()) {
                case JOB_REQUEST:
                    // processing job request
                    // ...
                    Object content = message.getContent();

                    Job job = (Job) content;

                    String toolName = job.getToolName();
                    
                    try
                    {
                        Tool tool = satellite.getToolObject(toolName);

                        Object parameters = job.getParameters();

                        Object result = tool.go(parameters);

                        writeToNet.writeObject((Integer)result);

                        writeToNet.flush();
                    }
                    catch (Exception e)
                    {
                        System.out.println("Something went wrong executing job");
                    }
                    
                    break;

                default:
                    System.err.println("[SatelliteThread.run] Warning: Message type not implemented");
            }

            try
            {
                jobRequest.close();
            }
            catch (IOException e)
            {
                System.out.println("Error closing job request");
            }
        }
    }

    /**
     * Aux method to get a tool object, given the fully qualified class string
     * If the tool has been used before, it is returned immediately out of the cache,
     * otherwise it is loaded dynamically
     */
    public Tool getToolObject(String toolClassString) throws UnknownToolException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        Tool toolObject = null;

        if ((toolObject = toolsCache.get(toolClassString)) == null) 
        {
            Class<?> toolClass = classLoader.loadClass(toolClassString);

            try 
            {
                toolObject = (Tool)toolClass.getDeclaredConstructor().newInstance();
            } 
            catch (InvocationTargetException | NoSuchMethodException ex) {
                System.err.println("getToolObject() - Exception");
            }

            toolsCache.put(toolClassString, toolObject);
        } 
        else 
        {
            System.out.println("Class: \"" + toolClassString + "\" already in Cache");
        }

        return toolObject;
    }

    public static void main(String[] args) {
        // start the satellite
        Satellite satellite = new Satellite(args[0], args[1], args[2]);
        satellite.run();
    }
}
