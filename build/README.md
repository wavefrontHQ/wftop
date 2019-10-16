# Create the executable file for windows
  * Install JAVA
  * Install Maven
  * Install innosetup
  * Install Git
  * Clone this repository `https://github.com/wavefrontHQ/wftop` and create 
  a jar file by running below command:
        
         mvn clean install -DskipTests
    
  * Create wftop directory in c drive
  * Copy below items into wftop directory
    * jre (jre1.8.0_221->jre)
    * installer icon (wavefront.ico)
    * innosetup script (setupScript.iss)
    * wftop jar file (wftop.jar)
    * cmd file (startWftop.cmd)
  * compile the inno script (setupScript.iss)
  * The executable file will be created in output directory (wftop.exe)