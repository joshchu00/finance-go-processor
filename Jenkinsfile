node {
  def processorImage

  stage('Git Pull') {
    git url: 'https://github.com/joshchu00/finance-go-processor.git', branch: 'develop'
  }
  stage('Go Build') {
    sh "${tool name: 'go-1.11', type: 'go'}/bin/go build -a -o main"
  }
  stage('Docker Build') {
    docker.withTool('docker-latest') {
      processorImage = docker.build('docker.io/joshchu00/finance-go-processor')
    }
  }
  stage('Docker Push') {
    docker.withTool('docker-latest') {
      docker.withRegistry('', 'DockerHub') {
        processorImage.push()
      }
    }
  }
}
