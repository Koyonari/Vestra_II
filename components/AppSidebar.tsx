'use client'

import { useState, useEffect } from 'react'
import { Settings, Moon, Sun, LineChart, User, LogOut, ChevronUp, Heart, UserCircle2 } from 'lucide-react'
import { useUser, useClerk } from '@clerk/nextjs'
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarFooter,
} from '@/components/ui/sidebar'
import { Switch } from '@/components/ui/switch'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { useSidebar } from '@/components/ui/sidebar'
import Image from 'next/image'

// Menu items
const items = [
  {
    title: 'Stock Dashboard',
    url: '',
    icon: LineChart,
    requiresAuth: false,
  },
  {
    title: 'Watchlist',
    url: 'watchlist',
    icon: Heart,
    requiresAuth: true,
  }
]

export function AppSidebar() {
  const { state, isMobile } = useSidebar()
  const { user, isSignedIn } = useUser()
  const { signOut, openSignIn } = useClerk()
  
  const [isProfileOpen] = useState(false)
  
  // Theme management
  const [isDarkMode, setIsDarkMode] = useState(false)
  const [mounted, setMounted] = useState(false)

  // Function to apply the theme
  const applyTheme = (darkMode: boolean) => {
    if (darkMode) {
      document.documentElement.classList.add('dark')
    } else {
      document.documentElement.classList.remove('dark')
    }
  }

  // Function to toggle between dark and light mode
  const toggleTheme = () => {
    const newMode = !isDarkMode
    setIsDarkMode(newMode)
    applyTheme(newMode)
  }

  useEffect(() => {
    setMounted(true)
    
    // Check system preference
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
    setIsDarkMode(prefersDark)
    applyTheme(prefersDark)

    // Listen for OS theme changes
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
    const handleChange = (e: MediaQueryListEvent) => {
      setIsDarkMode(e.matches)
      applyTheme(e.matches)
    }
    
    mediaQuery.addEventListener('change', handleChange)

    return () => {
      mediaQuery.removeEventListener('change', handleChange)
    }
  }, [])

  // Prevent hydration mismatch by not rendering theme-dependent content until mounted
  if (!mounted) {
    return null
  }

  const username = user?.fullName || user?.username || user?.primaryEmailAddress?.emailAddress || 'Guest'
  const userInitial = username.charAt(0).toUpperCase()

  const handleMenuClick = (item: typeof items[0], e: React.MouseEvent) => {
    if (item.requiresAuth && !isSignedIn) {
      e.preventDefault()
      openSignIn()
    }
  }

  const handleDropdownItemClick = (action: () => void) => {
    if (!isSignedIn) {
      openSignIn()
    } else {
      action()
    }
  }

  return (
    <div className={isDarkMode ? 'dark' : ''}>
      <Sidebar
        variant="floating"
        collapsible="icon"
        className="bg-background text-foreground transition-colors"
      >
        <SidebarContent>
          <SidebarGroup>
            <div className="text-sm italic mb-4 transition-colors duration-300">
              <Image src="/img/vestra.svg" alt="Vestra" width={64} height={64} className="h-16 w-16" />
            </div>
            <SidebarGroupContent>
              <SidebarMenu
                className={`flex flex-col gap-2 ${
                  state === 'collapsed' && !isMobile ? 'items-center' : ''
                }`}
              >
                {items.map((item) => {
                  const Icon = item.icon
                  const isLocked = item.requiresAuth && !isSignedIn
                  return (
                    <SidebarMenuItem key={item.title}>
                      <SidebarMenuButton asChild>
                        <a
                          href={item.url}
                          onClick={(e) => handleMenuClick(item, e)}
                          className={`flex items-center gap-2 px-2 py-2 rounded-md hover:bg-copper/10 transition-all duration-300 ${
                            isLocked ? 'opacity-80' : ''
                          }`}
                          title={isLocked ? 'Login required to access this feature' : undefined}
                        >
                          <Icon className="h-5 w-5 text-copper transition-colors duration-300 self-center" />
                          <span className="transition-colors duration-300 text-sm">{item.title}</span>
                        </a>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  )
                })}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
        
        <SidebarFooter className="border-t border-copper/30 pt-4 mt-auto transition-colors duration-300">
          <div
            className={`flex flex-col space-y-4 ${
              state === 'expanded' || isMobile
                ? 'px-3'
                : 'px-1 items-center'
            }`}
          >
            {/* Theme Toggle */}
            <div
              className={`flex items-center ${
                state === 'expanded' || isMobile
                  ? 'justify-between w-full'
                  : 'justify-center'
              }`}
            >
              {(state === 'expanded' || isMobile) && (
                <span className="text-sm transition-colors duration-300">Theme</span>
              )}
              <div className={`flex items-center ${state === 'expanded' || isMobile ? 'gap-2' : ''}`}>
                {state === 'collapsed' && !isMobile ? (
                  <button
                    onClick={toggleTheme}
                    className="flex items-center justify-center transition-colors duration-300"
                  >
                    {!isDarkMode ? (
                      <Sun className="h-4 w-4 text-copper transition-colors duration-300" />
                    ) : (
                      <Moon className="h-4 w-4 text-copper transition-colors duration-300" />
                    )}
                  </button>
                ) : (
                  <>
                    {!isDarkMode ? (
                      <Sun className="h-4 w-4 text-copper transition-colors duration-300" />
                    ) : (
                      <Moon className="h-4 w-4 text-copper transition-colors duration-300" />
                    )}
                    <Switch checked={isDarkMode} onCheckedChange={toggleTheme} />
                  </>
                )}
              </div>
            </div>

            {/* User Profile */}
            <DropdownMenu>
              <DropdownMenuTrigger
                className={`flex items-center rounded-md hover:bg-copper/10 cursor-pointer transition-colors duration-300 ${
                  state === 'expanded' || isMobile
                    ? 'justify-between p-2 w-full'
                    : 'justify-center p-1'
                }`}
              >
                <div className="flex items-center gap-2">
                  {isSignedIn && user?.imageUrl ? (
                    <Image
                      src={user.imageUrl}
                      alt={username}
                      width={32}
                      height={32}
                      className="h-8 w-8 rounded-full object-cover"
                    />
                  ) : isSignedIn ? (
                    <div className="h-8 w-8 rounded-full bg-copper flex items-center justify-center text-pale_white transition-colors duration-300">
                      {userInitial}
                    </div>
                  ) : (
                    <div className="h-8 w-8 rounded-full bg-muted flex items-center justify-center transition-colors duration-300">
                      <UserCircle2 className="h-5 w-5 text-muted-foreground" />
                    </div>
                  )}
                  {(state === 'expanded' || isMobile) && (
                    <span className="font-medium transition-colors duration-300">{username}</span>
                  )}
                </div>
                {(state === 'expanded' || isMobile) && (
                  <ChevronUp
                    className={`h-4 w-4 transition-transform duration-300 ${
                      !isProfileOpen ? 'rotate-180' : ''
                    }`}
                  />
                )}
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-56 transition-colors duration-300">
                <DropdownMenuItem 
                  className={`cursor-pointer transition-colors duration-300 ${
                    !isSignedIn ? 'opacity-80' : ''
                  }`}
                  onClick={() => handleDropdownItemClick(() => {})}
                  title={!isSignedIn ? 'Login required to access this feature' : undefined}
                >
                  <User className="mr-2 h-4 w-4 transition-colors duration-300" />
                  <span className="transition-colors duration-300">Account</span>
                </DropdownMenuItem>
                <DropdownMenuItem 
                  className={`cursor-pointer transition-colors duration-300 ${
                    !isSignedIn ? 'opacity-80' : ''
                  }`}
                  onClick={() => handleDropdownItemClick(() => {})}
                  title={!isSignedIn ? 'Login required to access this feature' : undefined}
                >
                  <Settings className="mr-2 h-4 w-4 transition-colors duration-300" />
                  <span className="transition-colors duration-300">Settings</span>
                </DropdownMenuItem>
                  {isSignedIn ? (
                  <DropdownMenuItem 
                    className="cursor-pointer text-red-500 transition-colors duration-300"
                    onClick={() => signOut()}
                  >
                    <LogOut className="mr-2 h-4 w-4 transition-colors duration-300" />
                    <span className="transition-colors duration-300">Sign out</span>
                  </DropdownMenuItem>
                ) : (
                  <DropdownMenuItem 
                    className="cursor-pointer text-copper transition-colors duration-300"
                    onClick={() => openSignIn()}
                  >
                    <User className="mr-2 h-4 w-4 transition-colors duration-300" />
                    <span className="transition-colors duration-300">Sign in</span>
                  </DropdownMenuItem>
                )}
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </SidebarFooter>
      </Sidebar>
    </div>
  )
}
