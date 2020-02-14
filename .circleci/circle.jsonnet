local circle = import 'circle.libsonnet';

circle.ServiceConfig('converter') {
  jobs+: {
    tests: circle.Job(dockerImage='tritonmedia/base', withDocker=false) {
      steps_+:: [
        circle.RestoreCacheStep('yarn-{{ checksum "package.json" }}'),
        circle.RunStep('Fetch Dependencies', 'yarn --frozen-lockfile'),
        circle.SaveCacheStep('yarn-{{ checksum "package.json" }}', ['node_modules']),
        circle.RunStep('Run Tests', 'yarn test')
      ],
    },
  },
  workflows+: {
    ['build-push']+: {
      jobs_:: [
        'tests', 
        {
          name:: 'build',
          requires: ['tests'],
        }
      ],
    },
  },
}