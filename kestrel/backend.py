import logging

log = logging.getLogger(__name__)


class Kestrel(object):

    def __init__(self, redis):
        self.redis = redis

    def job_id(self):
        p = self.redis.pipeline()
        p.incr('jobs:next_id', 1)
        p.get('jobs:next_id')
        result = p.execute()
        return result[1]

    def register_worker(self, name, capabilities):
        log.debug('POOL: Register %s' % name)
        capabilities = set([cap.upper() for cap in capabilities])
        jobs = self.redis.smembers('jobs:queued')
        worker_jobs = set()
        for job in jobs:
            reqs = self.redis.smembers('job:%s:requirements' % job)
            if reqs.issubset(capabilities):
                worker_jobs.add(job)

        p = self.redis.pipeline()
        for cap in capabilities:
            p.sadd('worker:%s' % name, cap)
            p.sadd('workers:capabilities', cap)
        for job in worker_jobs:
            p.sadd('worker:%s:jobs' % name, job)
            p.sadd('job:%s:workers' % job, name)
        p.sadd('workers:online', name)
        p.execute()

    def worker_available(self, name):
        log.debug('POOL: Worker %s available' % name)
        if self.redis.sismember('workers:online', name):
            p = self.redis.pipeline()
            p.srem('workers:busy', name)
            p.sadd('workers:available', name)
            p.execute()

            queued_jobs = self.redis.smembers('jobs:queued')
            worker_jobs = self.redis.smembers('worker:%s:jobs' % name)
            for job in worker_jobs:
                if job in queued_jobs:
                    task = self.redis.srandmember('job:%s:tasks:queued' % job)
                    if task is not None:
                        p = self.redis.pipeline()
                        p.smove('job:%s:tasks:queued' % job,
                                'job:%s:tasks:pending' % job,
                                task)
                        p.set('job:%s:task:%s:is_pending' % (job, task), 'True')
                        p.expire('job:%s:task:%s:is_pending' % (job, task), 15)
                        p.sadd('worker:%s:tasks' % name,
                               '%s,%s' % (job, task))
                        p.set('job:%s:task:%s' % (job, task), name)
                        p.execute()
                        log.debug('MATCH: Matched worker %s to ' % name + \
                                  'task %s,%s' % (job, task))
                        return job, task
            return None

    def worker_busy(self, name):
        log.debug('POOL: Worker %s busy' % name)
        if self.redis.sismember('workers:online', name):
            p = self.redis.pipeline()
            p.srem('workers:available', name)
            p.sadd('workers:busy', name)
            p.execute()

    def worker_offline(self, name):
        log.debug('POOL: Worker %s offline' % name)
        if self.redis.sismember('workers:online', name):
            p = self.redis.pipeline()
            p.srem('workers:online', name)
            p.srem('workers:available', name)
            p.srem('workers:busy', name)
            p.execute()

            reset_tasks = {}
            tasks = self.redis.smembers('worker:%s:tasks' % name)
            p = self.redis.pipeline()
            p.delete('worker:%s')
            p.delete('worker:%s:jobs' % name)
            p.delete('worker:%s:tasks' % name)
            for task in tasks:
                job, task = task.split(',')
                if job not in reset_tasks:
                    reset_tasks[job] = set()
                reset_tasks[job].add(task)

                p.smove('job:%s:tasks:pending' % job,
                        'job:%s:tasks:queued' % job,
                        task)
                p.set('job:%s:task:%s:is_pending' % (job, task), 'True')
                p.expire('job:%s:task:%s:is_pending' % (job, task), 15)
                p.smove('job:%s:tasks:running' %job,
                        'job:%s:tasks:queued' % job,
                        task)
                p.delete('job:%s:task:%s' % (job, task))
                log.debug('RESET: Resetting task %s,%s' % (job, task))
            p.execute()
            return reset_tasks

    def submit_job(self, job, owner, command, cleanup, size, requirements):
        log.debug('JOB: Job %s submitted by %s' % (job, owner))

        requirements = set(requirements)

        p = self.redis.pipeline()
        p.set('job:%s:owner' % job, owner)
        p.set('job:%s:command' % job, command)
        p.set('job:%s:cleanup' % job, command)
        p.set('job:%s:size' % job, size)
        for req in requirements:
            p.sadd('job:%s:requirements' % job, req)
        for task in xrange(0, int(size)):
            p.sadd('job:%s:tasks:queued' % job, task)
        p.sadd('jobs:queued', job)
        p.execute()

        p = self.redis.pipeline()
        matches = {}
        workers = self.redis.smembers('workers:online')
        for worker in workers:
            caps = self.redis.smembers('worker:%s' % worker)
            if caps.issuperset(requirements):
                p.sadd('job:%s:workers' % job, worker)
                p.sadd('worker:%s:jobs' % worker, job)

                if self.redis.sismember('workers:online', worker):
                    task = self.redis.srandmember('job:%s:tasks:queued' % job)
                    if task is not None:
                        p.smove('job:%s:tasks:queued' % job,
                                'job:%s:tasks:pending' % job,
                                task)
                        p.set('job:%s:task:%s:is_pending' % (job, task), 'True')
                        p.expire('job:%s:task:%s:is_pending' % (job, task), 15)
                        p.sadd('worker:%s:tasks' % worker,
                               '%s,%s' % (job, task))
                        p.set('job:%s:task:%s' % (job, task), worker)
                        log.debug('MATCH: Matched worker %s to ' % worker + \
                                  'task %s,%s' % (job, task))
                        matches[task] = worker
        p.execute()
        return job, matches

    def cancel_job(self, job, canceller):
        owner = self.redis.get('job:%s:owner' % job)
        if owner != canceller:
            return None
        log.debug('JOB: Job %s cancelled by %s' % (job, owner))
        self.redis.srem('jobs:queued', job)

        cancellations = {}
        tasks = self.redis.sunion(['job:%s:tasks:running' % job,
                                   'job:%s:tasks:pending' % job])
        self.redis.sunionstore('job:%s:tasks:completed',
                               ['job:%s:tasks:queued' % job,
                                'job:%s:tasks:completed' % job])
        for task in tasks:
            worker = self.redis.get('job:%s:task:%s' % (job, task))
            if worker and worker not in cancellations:
                cancellations[worker] = set()
            cancellations[worker].add(task)
        return cancellations

    def task_start(self, worker, job, task):
        log.debug('TASK: Task %s,%s started by %s' % (job, task, worker))
        p = self.redis.pipeline()
        p.smove('job:%s:tasks:pending' % job,
                'job:%s:tasks:running' % job,
                task)
        p.set('job:%s:task:%s:is_pending' % (job, task), 'True')
        p.expire('job:%s:task:%s:is_pending' % (job, task), 15)
        p.execute()

    def task_finish(self, worker, job, task):
        job, task = str(job), str(task)
        log.debug('TASK: Task %s,%s finished by %s' % (job, task, worker))
        p = self.redis.pipeline()
        p.smove('job:%s:tasks:running' % job,
                'job:%s:tasks:completed' % job,
                task)
        p.srem('worker:%s:tasks' % worker, '%s,%s' % (job, task))
        p.delete('job:%s:task:%s' % (job, task), worker)
        p.execute()
        job_size = int(self.redis.get('job:%s:size' % job))
        num_completed = self.redis.scard('job:%s:tasks:completed' % job)
        if num_completed == job_size:
            self.redis.smove('jobs:queued', 'jobs:completed', job)
            return True
        return False

    def task_reset(self, worker, job, task):
        log.debug('TASK: Task %s,%s for %s reset.' % (job, task, worker))
        p = self.redis.pipeline()
        p.smove('job:%s:tasks:pending' % job,
                'job:%s:tasks:queued' % job,
                task)
        p.smove('job:%s:tasks:running' % job,
                'job:%s:tasks:queued' % job,
                task)
        p.delete('job:%s:task:%s:is_pending' % (job, task))
        p.srem('worker:%s:tasks' % worker, '%s,%s' % (job, task))
        p.delete('job:%s:task:%s' % (job, task), worker)
        p.execute()

    def job_status(self, job=None):
        if job is None:
            jobs = self.redis.smembers('jobs:queued')
        else:
            jobs = [str(job)]
        statuses = {}
        for job in jobs:
            status = {'owner': self.redis.get('job:%s:owner' % job),
                      'requested': self.redis.get('job:%s:size' % job)}
            for group in ['queued', 'pending', 'running', 'completed']:
                key = 'job:%s:tasks:%s' % (job, group)
                status[group] = self.redis.scard(key)
            statuses[job] = status
        return statuses

    def pool_status(self):
        return {'online': self.redis.scard('workers:online'),
                'available': self.redis.scard('workers:available'),
                'busy': self.redis.scard('workers:busy')}

    def clean(self):
        p = self.redis.pipeline()
        p.sinterstore('workers:available',
                      keys=('workers:online', 'workers:available'))
        p.sinterstore('workers:busy',
                      keys=('workers:online', 'workers:busy'))
        p.execute()

    def user_jobs(self, user):
        jobs = self.redis.smembers('jobs:queued')
        user_jobs = set()
        for job in jobs:
            if self.redis.get('job:%s:owner' % job) == user:
                user_jobs.add(job)
        return user_jobs

    def known_worker(self, name):
        return self.redis.sismember('workers:online', name)

    def online_workers(self):
        return self.redis.smembers('workers:online')

    def available_workers(self):
        return self.redis.smembers('workers:available')

    def busy_workers(self):
        return self.redis.smembers('workers:busy')

    def get_jobs(self):
        jobs = self.redis.smembers('jobs:queued')
        user_jobs = {}
        for job in jobs:
            user_jobs[job] = self.redis.get('job:%s:owner' % job)
        return user_jobs

    def get_job(self, job):
        data = {}
        data['id'] = job
        data['owner'] = self.redis.get('job:%s:owner' % job)
        data['command'] = self.redis.get('job:%s:command' % job)
        data['cleanup'] = self.redis.get('job:%s:cleanup' % job)
        data['size'] = self.redis.get('job:%s:size' % job)
        data['requirements'] = self.redis.smembers('job:%s:requirements' % job)
        return data
