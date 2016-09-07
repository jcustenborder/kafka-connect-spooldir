#!groovy
node {
    def mvnBuildNumber = "0.1.${env.BUILD_NUMBER}"

    def mvnHome = tool 'M3'

    checkout scm

    if (env.BRANCH_NAME == 'master') {
        stage 'versioning'
        sh "${mvnHome}/bin/mvn -B versions:set -DgenerateBackupPoms=false -DnewVersion=${mvnBuildNumber}"
    }

    stage 'build'
    sh "${mvnHome}/bin/mvn -B -P maven-central clean verify package"

    junit '**/target/surefire-reports/TEST-*.xml'

    if (env.BRANCH_NAME == 'master') {
        stage 'publishing'
        sh "${mvnHome}/bin/mvn -B -P github,maven-central deploy"
    }
}