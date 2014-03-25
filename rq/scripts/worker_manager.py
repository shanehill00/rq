
import os
import argparse
import re
import time
import signal
import sys
import yaml
#from newsle.modules import logs

"""===========================================================
 * Class that handles process management
===========================================================
"""
class RQManager() :

	"""
	 * Log levels can be enabled from the command line with -v, -vv, -vvv, -vvvv
	"""
	LOG_LEVEL_INFO = 1
	LOG_LEVEL_PROC_INFO = 2
	LOG_LEVEL_WORKER_INFO = 3
	LOG_LEVEL_DEBUG = 4
	LOG_LEVEL_CRAZY = 5
	
	"""
	 * Creates the manager and gets things going
	"""
	def __init__(self) :

		"""
		number of times we have received a sig term_count
		if we get 5 SIGTERM then we use SIGKILL to kill the child procs
		"""
		self.term_count = 0

		""" 
		maximun number of SIGTERMs we receive before we use SIGKILL to kill the child procs 
		"""
		self.max_terms = 5

		"""
		 * Holds the worker configuration
		"""
		self.config = {}

		"""
		 * Boolean value that determines if the running code is the parent or a child
		"""
		self.isparent = True

		"""
		 * When True, workers will stop look for jobs and the parent process will
		 * kill off all running children
		"""
		self.stop_work = False

		"""
		 * The timestamp when the signal was received to stop working
		"""
		self.stop_time = 0

		"""
		 * The dict of running child processes
		"""
		self.children = {}

		"""
		 * When forking helper children, the parent waits for a signal from them
		 * to continue doing anything
		"""
		self.wait_for_signal = False

		"""
		 * the options passed from the command line
		"""
		self.args = None

		"""
		 * Parse command line options. Loads the config file as well
		"""
		self.getopt()

		""" 
		execute the command passed on the command line
		"""
		self.do_command()
		
		#self.log("Exiting")


	def do_command(self):
		cmd = self.args.command
		getattr(self,cmd)()
	
	def get_parent_pid(self):
		parent_pid = 0
		if os.path.isfile( self.args.pid_file ) :
			f = open( self.args.pid_file, "r")
			parent_pid = int(f.readline().strip())
			if parent_pid <= 0 :
				os._exit("bad parent pid! " + str(parent_pid) +" less than or equal to 0!\n")
		else :
			exit("parent pid file " + self.args.pid_file + " not found!\n")

		return parent_pid

	def restart(self):
		self.stop()
		self.start()

	def stop(self) :
		ppid = self.get_parent_pid()
		os.kill(ppid,signal.SIGTERM)
		# now we wait unti the parent process has exited
		while( True ) : 
			try:
				os.kill(ppid, 0)
			except OSError:
				break
			else:
				time.sleep( 1 )
	
	def start( self ):
		
		# daemonize if we need to
		self.daemonize()

		# write the pid
		self.write_pid()

		"""
		 Register signal listeners
		"""
		self.register_sig_handlers()
		
		print "Started with pid " + str( os.getpid() ) + "\n"

		self.bootstrap()

		# now start watching the workers
		self.watch()
		
		self.cleanup()		

	def cleanup( self ) :
		os.remove( self.args.pid_file )

	def watch( self ):
		"""
		 * Main processing loop for the parent process
		"""
		while( not self.stop_work or len(self.children) > 0 ) :
			"""
			 * Check for exited children
			"""
			exited_pid = os.waitpid( 0, os.WNOHANG )[0]

			"""
			 * We run other children, make sure this is a worker
			"""
			if exited_pid in self.children :
				"""
				 * If they have exited, remove them from the children array
				 * If we are not stopping work, start another in its place
				"""
				worker = self.children[exited_pid]
				self.children.pop( exited_pid, None)
				child_pid_file = self.args.pid_file + "." + str(exited_pid)
				
				if os.path.isfile( child_pid_file ) :
					os.remove( child_pid_file )

				# print( "Child " + str(exited_pid) + " exited (worker) \n" )
				
				if not self.stop_work :
					self.start_worker(worker)

			lapsed = (time.time() - self.stop_time)
			if self.stop_work and lapsed > 60 :
				 # print ("Children have not exited, killing. " + str(os.getpid()) + "\n" )
				self.stop_children(signal.SIGKILL)

			"""
			 * python will eat up your cpu if you don't have this
			"""
			sleeptime = 100000/1000000.0
			time.sleep(sleeptime)

	"""
	 * Bootstap a set of workers and any vars that need to be set
	"""
	def bootstrap(self) :
		for worker in self.config["workers"] :
			total_workers = int(worker["count"])
			for n in range(0, total_workers) :
				self.start_worker( worker )

	def start_worker(self, worker) :

		pid = os.fork()

		if pid == 0:
			self.isparent = False
			self.register_sig_handlers(False)
			self.start_lib_worker(worker)
			# self.log("Child exiting", RQManager.LOG_LEVEL_WORKER_INFO)
			os._exit(0)

		elif pid == -1:
			print "Could not fork"
			self.stop_work = true
			self.stop_children()

		elif pid > 0 :
			#print "Started child: " + str(pid)
			self.children[pid] = worker
			child_pid_file = self.args.pid_file + "." + str( pid )
			#print "writing pid file: " + child_pid_file
			f = open(child_pid_file,"w")
			f.write( str(pid) )
			f.close()

	"""
	this function is where we start listening for work on RQ
	"""
	def start_lib_worker(self, worker) :
		# Preload libraries
		# from newsle.core.models.orm.stat_configs import StatConfigs
		# Provide queue names to listen to as arguments to this script,
		# similar to rqworker
		from rq import Queue, Connection, Worker, use_connection
		import redis
		if "server" in worker and len( worker["server"] ) > 0 :
				
				server = worker["server"]
				host = server.get("host", "127.0.0.1")
				port = server.get("port", "6379")
				password = server.get("password", None)

				redis_conn = redis.StrictRedis(host=host, port=port, db=None,
							   password=password, unix_socket_path=None)
				
				use_connection(redis_conn)

		with Connection():
			queues = ["default"]
			if "queues" in worker and len( worker["queues"] ) > 0 :
				queues = worker["queues"]

			qs = map(Queue, queues)

			w = Worker(qs)
			w.work()

	def write_pid( self ):
		try:
			pidfile = open(self.args.pid_file, "wb")
			pidfile.write( str(os.getpid()) )
			pidfile.close()

		except:
			print "Unable to write PID to " + self.args.pid_file + '!'


	def daemonize( self ):
		if self.args.daemon :
			pid = os.fork()
			if( pid == 0 ):
				# in the child
				# become the session leader
				self.isparent = True
				os.setsid()
				os.umask(0)
				os.chdir("/")
				gid = os.getsid( os.getpid() )

				if gid > 0 :
					pid = os.fork()
					if pid == 0:
						"""
						for f in sys.stdout, sys.stderr:
							f.flush( )
						si = file(sys.stdin, 'r')
						so = file(sys.stdout, 'a+')
						se = file(sys.stderr, 'a+', 0)
						os.dup2(si.fileno( ), sys.stdin.fileno( ))
						os.dup2(so.fileno( ), sys.stdout.fileno( ))
						os.dup2(se.fileno( ), sys.stderr.fileno( ))
						"""
					else :
						os._exit(0)
				else :
					os._exit(0)
			else:
				os._exit(0)

	"""
	 * Registers the process signal listeners
	"""
	def register_sig_handlers( self, parent = True ):

		if parent :
			# self.log("Registering signals for parent", RQManager.LOG_LEVEL_DEBUG)
			signal.signal( signal.SIGTERM, self.handle_signal )
			signal.signal( signal.SIGINT,  self.handle_signal )
			signal.signal( signal.SIGUSR1,  self.handle_signal )
			signal.signal( signal.SIGUSR2,  self.handle_signal )
			signal.signal( signal.SIGCONT,  self.handle_signal )
			signal.signal( signal.SIGHUP,  self.handle_signal )
		else :
			# self.log("Registering signals for child", RQManager.LOG_LEVEL_DEBUG)
			signal.signal( signal.SIGTERM, self.handle_signal )


	"""
	 * Handles signals
	"""
	def handle_signal(self, signo, frame ) :

		if self.isparent == False :
			self.stop_work = True
		elif signo == signal.SIGUSR1 :
			self.show_help("No worker files could be found")
		elif signo == signal.SIGUSR2:
			self.show_help("Error validating worker functions")
		elif signo == signal.SIGCONT:
			self.wait_for_signal = False
		elif signo == signal.SIGINT or signo == signal.SIGTERM :
			print "\nShutting down...\n"
			self.stop_work = True
			self.stop_time = time.time()
			self.term_count += 1
			if self.term_count < self.max_terms :
				self.stop_children( signal.SIGTERM )
			else :
				self.stop_children( signal.SIGKILL )

		elif signo == signal.SIGHUP :
			#self.log("Restarting children", RQManager.LOG_LEVEL_PROC_INFO)
			self.stop_children( signal.SIGTERM )

	"""
	 * Stops all running children
	"""
	def stop_children( self, signo ) :
		#print("Stopping children\n")
		for pid, worker in self.children.items() :
			#print "Stopping child " + str(pid) + " (worker) with signal " + str( signo )
			os.kill(pid, signo)
			child_pid_file = self.args.pid_file + "." + str(pid)
			if os.path.isfile( child_pid_file ) :
				os.remove( child_pid_file )


	"""
	 * Parses the command line options
	"""
	def getopt( self ) :
		parser = argparse.ArgumentParser()
		parser.add_argument("-c", "--config",  required=True, help="</path/to/config> Worker configuration file")
		parser.add_argument("-d", "--daemon",  action="store_true", help="Daemon, detach and run in the background")
		parser.add_argument("-p", "--pid-file", default="/tmp/rqmanager.pid", help="</path/to/pid/file> File to which to write the process ID")
		parser.add_argument("-v", "--verbose", choices=["v","vv","vvv","vvvv"], help="Increase verbosity level by one")
		parser.add_argument("-k", "--command", default="start", choices=["start","stop","reload","restart"], help="<start|stop|reload|restart>	The commnd to run.  one of start, stop, restart, reload.  start is the default")

		args = parser.parse_args()

		if( args.verbose ):
			val = args.verbose
			if val == False:
				self.verbose = RQManager.LOG_LEVEL_INFO
			elif val =="v":
				self.verbose = RQManager.LOG_LEVEL_PROC_INFO
			elif val =="vv":
				self.verbose = RQManager.LOG_LEVEL_WORKER_INFO
			elif val =="vvv":
				self.verbose = RQManager.LOG_LEVEL_DEBUG
			elif val =="vvvv":
				self.verbose = RQManager.LOG_LEVEL_CRAZY
			
		if args.config and not os.path.isfile( args.config ) :
			sys.exit("Config file " + args.config + " not found.\n")
		else :
			self.config = yaml.load( file( args.config ) )

		self.args = args

mgr = RQManager()
