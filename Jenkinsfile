import groovy.json.JsonOutput
import hudson.tasks.test.AbstractTestResultAction
import hudson.tasks.junit.CaseResult

def slackNotificationChannel = 'invdb'
def changeLog = ""
def testSummary = ""

def notifySlack(text, channel, attachments) {
	def slackURL = 'https://hooks.slack.com/services/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'
	def jenkinsIcon = 'https://egret.saic.com/etc/jenkins-ci_36.png'
	def payload = JsonOutput.toJson([text: text,
									 channel: channel,
									 username: 'jenkins',
									 icon_url: jenkinsIcon,
									 attachments: attachments
	])
	sh "curl -kX POST --data-urlencode \'payload=${payload}\' ${slackURL}"
}

@NonCPS
def getChangesSinceLastSuccessfulBuild() {
	def commits = []
	def build = currentBuild
	while (build != null && build.result != 'SUCCESS') {
		build.changeSets.collect { changeSet ->
			(changeSet.items.collect { item ->
				// item.author returns committer instead of author so we will just collect the commit id to get author another way
				//log += "- ${item.msg} [${item.author}]\n"
				commits << item.commitId
			})
		}
		build = build.previousBuild
	}
	/* if (log == '') {
		log = 'No changes.\n'
	}
	log = log.replaceAll("['\"]", "")
	return log */
	return commits
}

@NonCPS
def getTestSummary = { ->
	def testResults = currentBuild.rawBuild.getAction(AbstractTestResultAction.class)
	def summary = ''
	if (testResults != null) {
		total = testResults.totalCount
		failed = testResults.failCount
		def failedTestMessage = ''
		if (failed > 0) {
			def failedTests = testResults.getFailedTests()
			for (CaseResult failedTest : failedTests) {
				failedTestMessage += "${failedTest.getSimpleName()}.${failedTest.getDisplayName()}\n"
			}
			currentBuild.result = 'UNSTABLE'
		}
		skipped = testResults.skipCount
		summary = 'Passed: ' + (total - failed - skipped)
		summary = summary + (', Failed: ' + failed)
		summary = summary + (', Skipped: ' + skipped)
		if (failed > 0) {
			summary = summary + "\n*${failed} Failed Test" + (failed == 1 ? '':'s') + ":*\n" + failedTestMessage
		}
	} else {
		summary = 'No tests found.'
	}
	return summary
}

def populateGlobalVariables = {
	//changeLog = getChangesSinceLastSuccessfulBuild()
	hashes = getChangesSinceLastSuccessfulBuild()
	for (String hash : hashes) {
		log = sh(
				script: "git --no-pager show -s --pretty='format: - %s [%an]%n' ${hash}",
				returnStdout: true
		)
		changeLog += log.replaceAll("['\"]", "")
	}
	if (changeLog == '') {
		changeLog = 'No changes.\n'
	}
	testSummary = getTestSummary()
}

node {
	try {
		stage('checkout') {
			git poll: true,
					url: 'https://ghg-gitlab-r8.corp.saic.com/gitlab/inventory/invdb-py.git',
					credentialsId: 'bd578b37-6743-445e-9f25-7865f910e847',
					branch: 'develop'
		}
		stage('clean/compile') {
			env.JAVA_HOME = "${tool 'JDK11'}"
			env.PATH = "${env.PATH}:/usr/local/bin"
			populateGlobalVariables()
		}

		// stage('code coverage') {
		// 	jacoco(deltaBranchCoverage: '.2', deltaClassCoverage: '.2', deltaComplexityCoverage: '.2', deltaInstructionCoverage: '.2', deltaLineCoverage: '.2', deltaMethodCoverage: '.2', sourceInclusionPattern: '**/*.java,**/*.groovy', changeBuildStatus: true)
		// }
		// stage('static-dependency-scan') {
		// 	dependencyCheck additionalArguments: '--disableRetireJS --disableAssembly --suppression "dependency-check-false-positives.xml"', odcInstallation: 'Dependency-Check'
		// 	dependencyCheckPublisher unstableTotalCritical: 5, unstableTotalHigh: 25
		// }
		stage('docker-build-deploy') {
			try {
				sh 'aws ecr get-login-password | docker login --username AWS --password-stdin 015137877991.dkr.ecr.us-east-1.amazonaws.com'
			} catch (e) {
			}
			sh 'docker build -t 015137877991.dkr.ecr.us-east-1.amazonaws.com/invdb-py:${BUILD_NUMBER} --pull .'
			sh 'docker tag 015137877991.dkr.ecr.us-east-1.amazonaws.com/invdb-py:${BUILD_NUMBER} 015137877991.dkr.ecr.us-east-1.amazonaws.com/invdb-py:latest'
			sh 'docker push 015137877991.dkr.ecr.us-east-1.amazonaws.com/invdb-py:${BUILD_NUMBER}'
			sh 'docker push 015137877991.dkr.ecr.us-east-1.amazonaws.com/invdb-py:latest'
			sh 'BUILD_NUMBER=${BUILD_NUMBER} docker stack deploy --with-registry-auth -c compose.yaml --prune invdb-py'
		}
		stage('notify') {
			populateGlobalVariables()
			def buildColor = currentBuild.result == 'SUCCESS' ? 'good' : 'warning'
			def buildStatus = currentBuild.result == 'SUCCESS' ? 'Success' : 'Unstable'
			def icon = currentBuild.result == 'SUCCESS' ? ':+1:' : ':confounded:'
			notifySlack("", slackNotificationChannel, [
					[
							title: "${icon} Inventory ELT (python) (${env.JOB_NAME}) - #${env.BUILD_NUMBER} ${buildStatus}",
							color: "${buildColor}",
							text: "*Changes:*\n${changeLog}",
							'mrkdwn_in': ['fields'],
							fields: [
								[title: 'Test Status:',
								value: "${testSummary}",
								short: true]
							]
					]
			])
		}
	} catch (e) {
		echo e.message
		cause = sh(
				script: '''
				curl -ksX GET https://ghg-jenkins/jenkins/job/invdb-py-pipeline/lastBuild/consoleText 2> /dev/null | grep -F [ERROR]
				''',
				returnStdout: true
		)
		if (cause == '') {
			cause = e.message
		}
		changeLog = getChangesSinceLastSuccessfulBuild()
		notifySlack('', slackNotificationChannel, [
				[
						title: ":-1: Inventory ELT (python) api (${env.JOB_NAME}) - #${env.BUILD_NUMBER} Failure",
						color: 'danger',
						text: "*Changes:*\n${changeLog}*Failure:*\n${cause}"
				]
		])
		emailext (
				subject: "Jenkins build failed: 'Inventory ELT (python) (${env.JOB_NAME}) - #${env.BUILD_NUMBER}'",
				body: "Jenkins build failed:\n${cause}\n${env.BUILD_URL}",
				to: 'vinegasc@saic.com',
				from: 'jenkins@saic.com'
		)
		currentBuild.result = 'FAILURE'
	}
}
