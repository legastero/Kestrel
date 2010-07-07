import logging
import Queue as queue
import threading
from database import *

class Backend(object):
    def __init__(self, db, xmpp):
        self.xmpp = xmpp
        self.db = db.session()
        self.thread = threading.Thread(name='db_queue',
                                       target=self.start_thread)
        self.queue = queue.Queue()

        self.roster = RosterBackend(self)
        self.workers = WorkerBackend(self)
        self.jobs = JobBackend(self)
        self.tasks = TaskBackend(self)

        self.thread.daemon = True
        self.thread.start()

    def start_thread(self):
        while True:
            reply, pointer, args = self.queue.get(block=True)
            result = pointer(*args)
            if reply is not None:
                reply.put(result)

    def query(self, pointer, args=None):
        if args is None:
            args = tuple()
        out = queue.Queue()
        self.queue.put((out, pointer, args))
        return out.get(block=True)

# ######################################################################

class RosterBackend(object):

    def __init__(self, backend):
        self.backend = backend
        self.db = self.backend.db
        self.query = self.backend.query

    # ------------------------------------------------------------------

    def get(self, owner):
        return self.query(self._get, (owner,))

    def _get(self, owner):
        items = self.db.query(RosterItem.jid).filter_by(owner=owner).all()
        self.db.commit()
        return [r[0] for r in items]

    # ------------------------------------------------------------------

    def states(self, owner):
        return self.query(self._states, (owner,))

    def _states(self, owner):
        items = self.db.query(RosterItem.jid, RosterItem.show).filter_by(owner=owner).all()
        self.db.commit()
        return [(r[0], r[1]) for r in items]

    # ------------------------------------------------------------------

    def state(self, owner):
        return self.query(self._state, (owner,))

    def _state(self, owner):
        item = self.db.query(RosterItem.show).filter_by(owner=owner).all()
        self.db.commit()
        if item:
            return item[0][0]
        return None

    # ------------------------------------------------------------------

    def set_state(self, owner, state):
        return self.query(self._set_state, (owner, state))

    def _set_state(self, owner, state):
        items = self.db.query(RosterItem).filter_by(owner=owner).all()
        for item in items:
            item.show = state
            self.db.merge(item)
        self.db.commit()

    # ------------------------------------------------------------------

    def jids(self):
        return self.query(self._jids, tuple())

    def _jids(self):
        items = self.db.query(RosterItem.owner).all()
        self.db.commit()
        return [r[0] for r in items]

    # ------------------------------------------------------------------

    def sub_to(self, owner, jid):
        return self.query(self._sub_to, (owner, jid))

    def _sub_to(self, owner, jid):
        result = self.db.query(RosterItem).filter_by(owner=owner, jid=jid, subscription_to=1).count() > 0
        self.db.commit()
        return result

    # ------------------------------------------------------------------

    def sub_from(self, owner, jid):
        return self.query(self._sub_from, (owner, jid))

    def _sub_from(self, owner, jid):
        result = self.db.query(RosterItem).filter_by(owner=owner, jid=jid, subscription_from=1).count() > 0
        self.db.commit()
        return result

    # ------------------------------------------------------------------

    def has_sub(self, owner, jid):
        return self.query(self._has_sub, (owner, jid))

    def _has_sub(self, owner, jid):
        result = self.db.query(RosterItem).filter_by(owner=owner, jid=jid).count() > 0
        self.db.commit()
        return result

    # ------------------------------------------------------------------

    def clean(self):
        logging.info("Cleaning roster table.")
        return self.query(self._clean, tuple())

    def _clean(self):
        old = self.db.query(RosterItem).filter_by(subscription_to=0, subscription_from=0).all()
        for item in old:
            self.db.delete(item)
        self.db.commit()

    # ------------------------------------------------------------------

    def subscribe(self, owner, jid):
        return self.query(self._subscribe, (owner, jid))

    def _subscribe(self, owner, jid):
        entry = RosterItem(owner, jid)
        entry.subscribe()
        self.db.merge(entry)
        self.db.commit()

    # ------------------------------------------------------------------

    def subscribed(self, owner, jid):
        return self.query(self._subscribed, (owner, jid))

    def _subscribed(self, owner, jid):
        entry = RosterItem(owner, jid)
        entry.subscribed()
        self.db.merge(entry)
        self.db.commit()

    # ------------------------------------------------------------------

    def unsubscribe(self, owner, jid):
        return self.query(self._unsubscribe, (owner, jid))

    def _unsubscribe(self, owner, jid):
        entry = RosterItem(owner, jid)
        entry.unsubscribe()
        self.db.merge(entry)
        self.db.commit()

    # ------------------------------------------------------------------

    def unsubscribed(self, owner, jid):
        return self.query(self._unsubscribed, (owner, jid))

    def _unsubscribed(self, owner, jid):
        entry = RosterItem(owner, jid)
        entry.unsubscribed()
        self.db.merge(entry)
        self.db.commit()

# ######################################################################

class WorkerBackend(object):

    def __init__(self, backend):
        self.backend = backend
        self.db = self.backend.db
        self.query = self.backend.query

    # ------------------------------------------------------------------

    def status(self):
        return self.query(self._status, tuple())

    def _status(self):
        available = self.db.query(Worker).filter_by(state='available').count()
        busy = self.db.query(Worker).filter_by(state='busy').count()
        return {'online': str(available + busy),
                'available': str(available),
                'busy': str(busy)}

    # ------------------------------------------------------------------

    def add(self, jid, capabilities):
        return self.query(self._add, (jid, capabilities))

    def _add(self, jid, capabilities):
        worker = Worker(jid, capabilities, state='offline')
        self.db.merge(worker)
        self.db.commit()

    # ------------------------------------------------------------------

    def set_state(self, jid, state):
        return self.query(self._set_state, (jid, state))

    def _set_state(self, jid, state):
        worker = self.db.query(Worker).filter_by(jid=jid).one()
        if worker.state == state:
            self.db.commit()
            return False
        worker.state = state
        self.db.merge(worker)
        self.db.commit()
        return True

    # ------------------------------------------------------------------

    def known(self, jid):
        return self.query(self._known, (jid,))

    def _known(self, jid):
        result = self.db.query(Worker).filter_by(jid=jid).count() == 1
        self.db.commit()
        return result

    # ------------------------------------------------------------------

    def clean(self):
        return self.query(self._clean, tuple())

    def _clean(self):
        logging.info('Cleaning worker table.')
        self.db.query(Worker).delete()
        self.db.commit()

    # ------------------------------------------------------------------

    def match(self, worker_jid):
        return self.query(self._match, (worker_jid,))

    def _match(self, worker_jid):
        worker = self.db.query(Worker).filter_by(jid=worker_jid).one()
        caps = '%'+worker.capabilities.replace(' ', '%') + '%'

        where = and_(or_(Job.status=='queued',
                         Job.status=='running'),
                     Worker.capabilities.like(Job.requirements),
                     Worker.jid==worker_jid,
                     Worker.state=='available')
        job = self.db.query(Job).join((Worker, Worker.jid==worker_jid)).filter(where).first()
        if job is None:
            return False
        task = self.db.query(Task).filter_by(job_id=job.id, status='queued').first()
        if task is None:
            return False
        task.worker = worker
        task.status = 'pending'
        self.db.merge(task)
        self.db.commit()
        return task

    # ------------------------------------------------------------------

    def reset(self, worker_jid):
        return self.query(self._reset, (worker_jid,))

    def _reset(self, worker_jid):
        tasks = self.db.query(Task).filter_by(worker_id=worker_jid).all()
        jobs = set()
        for task in tasks:
            task.reset()
            task.worker_id = None
            jobs.add(task.job_id)
            self.db.merge(task)
        self.db.commit()
        return jobs


# ######################################################################

class JobBackend(object):

    def __init__(self, backend):
        self.backend = backend
        self.db = self.backend.db
        self.query = self.backend.query

    # ------------------------------------------------------------------

    def status(self, job_id=None):
        return self.query(self._status, (job_id,))

    def _status(self, job_id=None):
        if job_id is None:
            statuses = {}
            jobs = self.db.query(Job).filter(and_(Job.status!='completed',
                                                  Job.status!='cancelled')).all()
            for job in jobs:
                status = self._job_status(job.id)
                if status:
                    statuses[job.id] = status
            return statuses
        else:
            return self._job_status(job_id)

    def _job_status(self, job_id):
        job = self.db.query(Job).filter_by(id=job_id).first()
        if job is None:
            return False

        requested = job.queue
        queued = self.db.query(Task).filter_by(job_id=job_id, status='queued').count()
        pending = self.db.query(Task).filter_by(job_id=job_id, status='pending').count()
        running = self.db.query(Task).filter_by(job_id=job_id, status='running').count()
        cancelling = self.db.query(Task).filter_by(job_id=job_id, status='cancelling').count()
        cancelled = self.db.query(Task).filter_by(job_id=job_id, status='cancelled').count()
        completed = self.db.query(Task).filter_by(job_id=job_id, status='completed').count()

        return {'owner': job.owner,
                'requested': requested,
                'queued': queued + pending,
                'running': running,
                'completed': cancelling + cancelled + completed}

    # ------------------------------------------------------------------

    def queue(self, owner, command, jid=None, cleanup=None, queue=1, requires=None):
        return self.query(self._queue, (owner, command, jid, cleanup, queue, requires))

    def _queue(self, owner, command, jid=None, cleanup=None, queue=1, requires=None):
        job = Job(owner, command,
                  jid=jid,
                  status='queued',
                  cleanup=cleanup,
                  queue=queue,
                  requires=requires)
        self.db.add(job)
        self.db.commit()
        return job.id

    # ------------------------------------------------------------------

    def create_jid(self, job_id, base_jid, task_id=None):
        template = 'job_%d@%s'
        if task_id is None:
            return template % (job_id, base_jid)
        else:
            template += '/%d'
            return template % (job_id, base_jid, task_id)

    # ------------------------------------------------------------------

    def get_id(self, job_jid):
        return job_jid[job_jid.index('job_')+4:job_jid.index('@')]

    # ------------------------------------------------------------------

    def set_jid(self, job_id, job_jid):
        return self.query(self._set_jid, (job_id, job_jid))

    def _set_jid(self, job_id, job_jid):
        job = self.db.query(Job).filter_by(id=job_id).all()
        if job:
            job = job[0]
            job.jid = job_jid
            self.db.merge(job)
        self.db.commit()

    # ------------------------------------------------------------------

    def cancel(self, owner, job_id):
        return self.query(self._cancel, (owner, job_id))

    def _cancel(self, owner, job_id):
        job = self.db.query(Job).filter_by(owner=owner, id=job_id).all()
        if job:
            job = job[0]
            job.status = 'cancelled'
            self.db.merge(job)
            self.db.commit()
            self.backend.roster._set_state(job.jid, 'dnd')
            tasks = []
            for task in job.tasks:
                if task.status == 'running' and task.worker is not None:
                    tasks.append(task)
                task.cancel()
                self.db.merge(task)
            self.db.commit()
            if tasks:
                return tasks
            return True
        self.db.commit()
        return False

    # ------------------------------------------------------------------

    def match(self, job_id):
        return self.query(self._match, (job_id,))

    def _match(self, job_id):
        job = self.db.query(Job).filter_by(id=job_id).one()
        reqs = '%'+job.requirements.replace(' ', '%') + '%'
        result = self.db.query(Worker).filter(and_(Worker.state=='available',
                                                   Worker.capabilities.like(reqs)))
        self.db.commit()
        tasks = []
        for worker in result:
            task = self.db.query(Task).filter_by(job_id=job_id, status='queued').first()
            if task:
                task.worker = worker
                task.status = 'pending'
                self.db.merge(task)
                self.db.commit()
                tasks.append(task)
        return tasks
        
# ######################################################################

class TaskBackend(object):

    def __init__(self, backend):
        self.backend = backend
        self.db = self.backend.db
        self.query = self.backend.query

    # ------------------------------------------------------------------

    def clean(self):
        return self.query(self._clean, tuple())

    def _clean(self):
        logging.info('Cleaning tasks table')
        tasks = self.db.query(Task).filter(or_(Task.status=='pending',
                                               Task.status=='cancelling')).all()
        for task in tasks:
            task.reset()
            self.db.merge(task)

    # ------------------------------------------------------------------

    def finish(self, job_id, task_id):
        return self.query(self._finish, (job_id, task_id))

    def _finish(self, job_id, task_id):
        task = self.db.query(Task).filter_by(job_id=job_id, task_id=task_id).one()
        task.finish()
        self.db.merge(task)
        self.db.commit()
        job = self.db.query(Job).filter_by(id=job_id).one()
        unfinished = self.db.query(Task).filter(Task.job_id==job_id).filter(Task.status!='completed').count()
        if unfinished == 0:
            job.complete()
            self.db.merge(job)
            self.db.commit()
            self.backend.roster._set_state(job.jid, 'xa')
            return True
        running = self.db.query(Task).filter(Task.job_id==job_id).filter(Task.status=='running').count()
        if running > 0:
            job.status = 'running'
            self.db.merge(job)
            self.db.commit()
            self.backend.roster._set_state(job.jid, 'chat')
        else:
            job.status = 'queued'
            self.db.merge(job)
            self.db.commit()
            self.backend.roster._set_state(job.jid, 'away')
        return False

    # ------------------------------------------------------------------

    def start(self, job_id, task_id):
        return self.query(self._start, (job_id, task_id))

    def _start(self, job_id, task_id):
        task = self.db.query(Task).filter_by(job_id=job_id, task_id=task_id).one()
        task.start()
        task.job.status = 'running'
        self.db.merge(task)
        self.db.merge(task.job)
        self.db.commit()
        self.backend.roster._set_state(task.job.jid, 'chat')

    # ------------------------------------------------------------------

    def reset(self, job_id, task_id):
        return self.query(self._start, (job_id, task_id))

    def _reset(self, job_id, task_id):
        task = self.db.query(Task).filter_by(job_id=job_id, task_id=task_id).one()
        task.reset()
        self.db.merge(task)
        self.db.commit()


