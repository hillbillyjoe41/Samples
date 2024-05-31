"""

engine repsonsibilities:
- receive control from relay
- receive model/sim details from config
- control modules
- transmit sim data to relay

"""

import importlib.util
import os
import sys
import datetime
from time import sleep
from config_manager import ConfigManager
from server_config_manager import ServerConfigManager
from socket_server import SocketServer
from process_manager import ProcessManager
from measures import Time, TimedData

class Engine():
	def __init__(self, project_path):
		print("ENGINE: initializing")

		# read config for server-related stuff
		self.address = (
			ServerConfigManager.read_value("server.engine.host"),
			ServerConfigManager.read_value("server.engine.port"))
		self.max_connections = ServerConfigManager.read_value("server.engine.max_connections")

		# other socket-related stuff
		self.first_connection = True
		self.connections = []

		# create socket server and start listening
		self.server = SocketServer(self.connection_handler, "engine-server")
		self.server.listen(self.address, self.max_connections)

		# other stuff
		self.live = False	# sim started
		self.active = False	# sim not paused (playing)
		self.modules = {}
		self.project_path = project_path
		self.modules_path = os.path.join(project_path, "modules/")
		self.message_queue = []

		# append modules/ folder in project directory to path
		# (to have imports in user modules working later)
		sys.path.append(self.modules_path)

	def connection_handler(self, client):
		# accepts (client_socket, client_address) tuple
		# returns (message_handler, thread_name) tuple
		print(f"ENGINE: connection accepted ({client[1]})")
		self.connections.append(client)

		# interface
		if(self.first_connection):
			# relay
			print("ENGINE: relay connected")
			self.first_connection = False
			self.server.send(client, { "msg": "kicking things off!" })
			to_return = (self.handle_relay, "engine-relay")
		else:
			# module
			print("ENGINE: module connected")
			to_return = (self.handle_module, "engine-module")
		
		return to_return

	def handle_relay(self, client, data):
		print(f"ENGINE: data from relay received!\n\t{data}")

		match data["type"]:
			case "sim_ctrl":
				match data["action"]:
					case "start":
						self.start_simulation()
						return
					case "stop":
						self.stop_simulation()
						return
					case "pause":
						if not self.active: return

						# FIXME
						self.live = False
						return
					case "resume":
						if not self.active: return

						# FIXME
						self.live = True

						# send all messages in queue
						for (module_name, message) in self.message_queue:
							self.server.send(self.modules[module_name]["connection"], message)
							self.modules[module_name]["waiting"] = False

						# clear message queue after sending
						self.message_queue = []
						return
				return

	def handle_module(self, client, data):
		print_data = data.copy()
		for (key, value) in print_data.items():
			if isinstance(value, Time): print_data[key] = vars(value)
		print(f"ENGINE: data from module received!\n\t{print_data}")
		#print(f"ENGINE: data from module received!\n\t{data}")
		if not self.active:
			# TODO: kill this module (after dumping its data)
			return

		# TODO: move all this stuff into a function of "engine-stored module" class mentioned somewhere else
		match data["type"]:
			case "variables":
				# {
				#	"type": "variables",
				#	"name": <module name>,
				#	"time": <time when the variables obtain these values>,
				#	"variables": {
				#		"variable_name": <value>,
				#		...
				#	}
				# }

				name = data["name"]
				endtime = data["time"]
				variables = data["variables"]
				module = self.modules[name]
				ts = module["timestep"]
				min_time = self.min_mod_time[0]
				min_module = self.min_mod_time[1]

				if(endtime < min_time): 
					self.min_mod_time = endtime, module
					min_time = self.min_mod_time[0]
					min_module = self.min_mod_time[1]

				elif(module == min_module): 
					self.min_mod_time = endtime, module
					min_time = self.min_mod_time[0]
					min_module = self.min_mod_time[1]

				# mark as waiting
				module["waiting"] = True

				# save variable data
				module["variables"].insert(endtime, variables)
				
				# remove time from times
				# FIXME: loop is weird
				for (i, time) in enumerate(module["target_times"]):
					if endtime == time:
						# if times is empty, insert next timestep
						module["target_times"].insert(0, endtime + ts)

						# then delete
						del module["target_times"][i + 1]
						break

				# outputs data to output sim folder every 100 s
				#fix outputs
				if min_time % Time(100, "s") == Time(0, "s"):
					numsky = endtime // Time(100, "s")
					numsky = numsky-1
					# Create a new file within the folder
					file_path = os.path.join(self.sim_folder_name, "output_sim" + str(numsky))
					with open(file_path, "a") as f:
						f.write("\n")
						f.write(name)
						f.write("\n")
						module["variables"].Write(list(variables.keys())[0], f, numsky*100 - 1)

				# clear and update parameter data, check if ready
				# FIXME: if a message from another module comes in before this parameter-clearing thing, they might get locked forever. should routinely gather and check parameters in a timed loop elsewhere to avoid this
				ready = True
				target_time = module["target_times"][0]
				minus = 1 if (target_time % ts).quantity == 0 else 0
				need_time = Time((target_time // ts - minus) * ts.quantity, ts.unit)
				module["parameters"] = {}
				for (smn, vp_mapping) in module["parameter_sources"].items():
					smv = self.modules[smn]["variables"]
					svt = smv.times[smv.find_nti(target_time)]
					if need_time <= svt and svt < target_time:
						sv = smv.get(need_time, vp_mapping.keys())
						for (svn, pn) in vp_mapping.items():
							module["parameters"][pn] = sv[svn]
					else:
						for (svn, pn) in vp_mapping.items():
							module["parameters"][pn] = None
						ready = False

				# if ready, simulate
				if ready:
					# determine how long to simulate for
					steps = (target_time - need_time) / ts
					
					# make simulation message
					sim_msg = {
						"type": "simulate",
						"target_time": target_time,
						"steps": steps,
						"raw_parameters": module["parameters"] }
					
					# simmsg: send to module or save to queue, depending on if sim is unpaused
					if self.live:
						# actually send sim message
						self.server.send(module["connection"], sim_msg)
						
						# mark as no longer waiting
						module["waiting"] = False
					else:
						# save sim message to queue
						self.message_queue.append((name, sim_msg))
				
				# update waiting dependent modules
				for dependent_module_name in self.get_dependent(module["name"]):
					dependent_module = self.modules[dependent_module_name]
					
					# skip if not waiting for variables
					if not dependent_module["waiting"]: continue
					
					# skip if new variable time too far from needed parameter time
					d_ts = dependent_module["timestep"]
					d_target_time = dependent_module["target_times"][0]
					d_minus = 1 if (d_target_time % d_ts).quantity == 0 else 0
					d_need_time = Time((d_target_time // d_ts - d_minus) * d_ts.quantity, d_ts.unit)
						# TODO: when class is made, cache this
					if d_need_time > endtime or endtime >= d_target_time: continue

					# insert new variable data into dependent module's parameters object
					for (vn, pn) in dependent_module["parameter_sources"][name].items():
						dependent_module["parameters"][pn] = variables[vn]
					
					# skip if not ready to simulate
					if None in dependent_module["parameters"].values(): continue
					
					# determine how long to simulate for
					d_steps = (d_target_time - d_need_time) / d_ts
					
					# make simulation message
					sim_msg = {
						"type": "simulate",
						"target_time": d_target_time,
						"steps": d_steps,
						"raw_parameters": dependent_module["parameters"] }
					
					# simmsg: send to module or save to queue, depending on if sim is unpaused
					if self.live:
						# actually send sim message
						self.server.send(dependent_module["connection"], sim_msg)
						
						# mark as no longer waiting
						dependent_module["waiting"] = False
					else:
						# save sim message to queue
						self.message_queue.append((dependent_module_name, sim_msg))

				# send variable data to relay (to pass to interface)
				self.server.send(self.connections[0], {
					"module": name,
					"times": list(map(lambda x: x.to("s").quantity, module["variables"].times)),
					"variables": module["variables"].variables })

				return

			case "detections":
				# {
				#	"type": "detections",
				#	"name": <module_name>,
				#	"time": <number>,
				#	"equilibrium": <true/false>,
				#	"significant_change": <true/false>,
				#	"variables": {
				#		"variable_name": <value>,
				#		...
				#	}
				# }

				# TODO
				if data["significant_change"]:
					pass
				elif data["equilibrium"]:
					pass
				# else?
				return

			case "greeting":
				# {
				# 	"type": "greeting",
				#	"name": <module name>
				# }

				# find matching connection
				name = data["name"]
				(socket, address) = client
				index = -1
				for save in self.connections:
					index += 1
					if save[1] == address: break

				# save it in modules object if found
				if index != -1:
					self.modules[name]["connection"] = client

				# flag as ready for simulation
				self.modules[name]["waiting"] = True
				return

	def stop_simulation(self):
		if not self.active: return
		# FIXME probably
		# stop things
		self.live = False
		self.active = False

		# TODO: dump variable data
		# //

		# TODO: kill old stuff
		for module in self.modules.values():
			ProcessManager.destroy(module["process"])

	def start_simulation(self):
		if self.active: return

		# FIXME probably
		# TODO: kill old stuff (the simulation should already be stopped, so just check instead?)
		# //

		# TODO: check valid config
		# //

		# extract data from config
		config_modules = ConfigManager.read_value("model.modules")

		#creates simulation folder
		self.create_sim_folder()

		# init module information
		self.modules = {}

		#keep track of min module time
		self.min_mod_time = Time(99999, "s"), "min"

		# init dependant module names for each variable
		self.dependent_module_names = {}

		# TODO: build dependency graph
		# nodes are modules, directed edges point from source (var) to target (param) and are variables

		# start modules
		# TODO: make engine-stored module objects a class instead of a dictionary
		for (i, config_module) in enumerate(config_modules):
			# required info for engine
			name = config_module["name"]

			filepath = os.path.join(self.project_path, "modules/", config_module["filepath"])

			ts_quantity = config_module["timestep"]["quantity"]
			ts_unit = config_module["timestep"]["unit"]
			ts = Time(ts_quantity, ts_unit)

			# store initial values of parameters in parameters ({ "<parameter_name>": <initial_value>, ... })
			# store source of parameters in parameter_sources ({ "<source_module_name>": { "<source_variable_name>": "<parameter_name>", ... }, ... })
			parameters = {}
			parameter_sources = {}
			for parameter in config_module["parameters"]:
				pn = parameter["name"]
				(smn, svn) = parameter["variable"].values()
				
				parameters[pn] = parameter["init_value"]
				
				if smn in parameter_sources:
					parameter_sources[smn][svn] = pn
				else:
					parameter_sources[smn] = { svn: pn }

			variables = {}
			for variable in config_module["variables"]:
				# TODO: initialize with actual initial values (defined in config), maybe?
				variables[variable["name"]] = 0

			module = {
				# [constant] model info
				"name": name,
				"timestep": ts,
				"parameter_sources": parameter_sources,
				
				# [dynamic] model mgmt
				"sleeping": False,
				"target_times": [ts],
				"parameters": parameters,
				"variables": TimedData(variables),

				# [const] module info
				"filepath": filepath,
				"process": -1,
				"connection": None,
				
				# [dyn] module mgmt
				"waiting": False
			}

			# start process
			process_identifier = ProcessManager.spawn(
				self.start_module,
				(filepath, name, ts),
				f"module-{name}")

			# TODO: check if process failed to start

			# save module info
			module["process"] = process_identifier
			self.modules[name] = module

		# start simulation loop
		self.active = True
		self.live = True

		# wait until all modules ready before starting sim convos
		while self.active:
			sleep(1)
			bad = False in map(lambda x: x["waiting"], self.modules.values())
			if not bad: break

		# in case they paused during initialization period after starting
		while self.active and not self.live:
			sleep(1)

		# start sim convos		
		for module in self.modules.values():
			self.server.send(module["connection"], {
				"type": "simulate",
				"target_time": module["target_times"][0],
				"steps": 1,
				"raw_parameters": module["parameters"] })
			module["waiting"] = False

	def start_module(self, filepath, name, timestep):
		print(f"ENGINE: starting module at {filepath}")

		# import module
		# (https://stackoverflow.com/a/67692)
		spec = importlib.util.spec_from_file_location(name, filepath)
		pymod = importlib.util.module_from_spec(spec)
		sys.modules[name] = pymod
		spec.loader.exec_module(pymod)

		# start module
		pymod.Module(self.address)

	def enter_simulation_loop(self):
		while self.active:
			sleep(5)
			
			# check if a module is needlessly waiting
			for module in filter(lambda x: x["waiting"], self.modules.values()):
				if not (None in module["parameters"].values()) and len(module["target_times"]) > 0:
					# FIXME: should probs handle if times is empty, but i don't think it can happen
					ts = module["timestep"]
					target_time = module["target_times"][0]
					minus = 1 if target_time // ts == target_time / ts else 0
					need_time = Time((target_time // ts - minus) * ts.quantity, ts.unit)
					steps = (target_time - need_time) / ts
					self.server.send(module["connection"], {
						"type": "simulate",
						"target_time": target_time,
						"steps": steps,
						"raw_parameters": module["parameters"] })
			
			# TODO: tell modules to dump their variable data
			# //

	def get_dependent(self, module_name):
		dependent_module_names = []
		for (name, module) in self.modules.items():
			if name == module_name: continue
			if module_name in module["parameter_sources"]:
				dependent_module_names.append(name)
		return dependent_module_names

	def get_var_modules(self, module_name, var_name):
		#checks if module_name and var_name are present
		if module_name in self.dependent_module_names:
			if var_name in self.dependent_module_names[module_name]:
				#returns if variable found
				return self.dependent_module_names[module_name[var_name]]
			#if something is not found we add it to the dictionary
			else:
				self.dependent_module_names[module_name].append(var_name)
		else:
			self.dependent_module_names.append(module_name)
			self.dependent_module_names[module_name].append(var_name)

		#adds values to dictionary for which modules are dependant upon that variable
		for module in self.modules.values():
			for (key, var_list) in module["parameter_sources"].items():
				if key == module_name:
					if var_name in var_list:
						self.dependent_module_names[module_name[var_name]].append(module)
		return self.dependant_module_names[module_name[var_name]]
	
	def create_sim_folder(self):
		# Get the directory path of the current script
		# fix path
		script_dir = self.project_path

		#creates folder with current time as name
		current_time = datetime.datetime.now()
		current_time_formatted = current_time.strftime("%Y-%m-%d-%H-%M-%S")
		string = "output_sim_folder-"
		self.sim_folder_name = os.path.join(script_dir, "outputs", string+current_time_formatted)

		try:
			os.makedirs(self.sim_folder_name)
		except OSError as e:
			print("Failed to create folder '{}' due to: {}".format(self.sim_folder_name, e))
