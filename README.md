# Vault Java SDK Sample - vsdk-job-service-sample

**Please see the [project wiki](https://github.com/veeva/vsdk-job-service-sample/-/wikis/home) for a detailed walkthrough**.

The **vsdk-job-service-sample** project covers the use of the SDK job Service with SDK Triggers. The Trigger will accomplish the following:

-   Validate that the product exists and show a translated error in French if it does not.
-   Validate that the order quantity is less than the quantity in stock and show a translated error in French if it does not.

## How to import

Import the project as a Maven project. This will automatically pull in the required Vault Java SDK dependencies. 

For Intellij this is done by:
-	File -> Open -> Navigate to project folder -> Select the 'pom.xml' file -> Open as Project

For Eclipse this is done by:
-	File -> Import -> Maven -> Existing Maven Projects -> Navigate to project folder -> Select the 'pom.xml' file


## Setup

For this project, the custom trigger and necessary vault components are contained in the two separate vault packages (VPK). The VPKs are located in the project's **deploy-vpk** directory  and **need to be deployed to your vault** prior to debugging these use cases:

1.  Clone or download the sample Maven project [vSDK job Service Sample project](https://github.com/veeva/vsdk-job-service-sample) from Github.
2.  Run through the [Getting Started](https://developer.veevavault.com/sdk/#getting-started) guide to set up your development environment.
3.  Log in to your vault and navigate to **Admin > Deployment > Inbound Packages** and click **Import**:
4.  Locate and select the following file in your downloaded project file:

    > **Custom Trigger** code: **\deploy-vpk\code\vsdk-job-service-sample-code.vpk** file.
 
5.  From the **Actions** menu (gear icon), select **Review & Deploy**. Vault displays a list of all components in the package.   
6.  Review the prompts to deploy the package. You will receive an email when vault completes the deployment.
7.  Repeat steps 3-6 for the vault components, select the package that matches your vault type:

    >Deploy vault components: Select the \deploy-vpk\components\vsdk-job-service-sample-components.vpk file.

## Notes 

The Jobmetadata Component configuration is only accessible via MDL.                                                                                              
 
## License

This code serves as an example and is not meant to be used for production use.

Copyright 2020 Veeva Systems Inc.
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
