pipeline {
  agent { label 'ubuntu' }
  stages {
    stage('pre') {
      steps {
        echo 'start'
      }
    }
    stage('build') {
      parallel {
        stage('JDK 8') {
          tools {
	    jdk 'JDK 1.8 (latest)'
	  }
	  steps {
            sh 'gradle -version'
          }
        }

        stage('JDK 11') {
          tools {
	    jdk 'JDK 11 (latest)'
	  }
	  steps {
            sh 'gradle -version'
            pullRequest.createStatus('SUCCESS', 'JDK 11 build', 'Does this work?')
          }
        }
      }
    }
    stage('post') {
      steps {
        echo 'finish'
      }
    }
  }
}
